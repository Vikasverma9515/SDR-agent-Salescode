"""
SCAI ProspectOps CLI and API server entrypoint.

CLI usage:
  python -m backend.main fini --companies "Company A,Company B" --sdr "SDR Name" --submit-n8n
  python -m backend.main searcher --companies "Company A"
  python -m backend.main veri
  python -m backend.main status --thread-id <id>
  python -m backend.main resume --thread-id <id>
  python -m backend.main ui  # Start the web UI server
"""
from __future__ import annotations

import asyncio
import json
import sys
import uuid
from typing import Optional

import typer
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich import print as rprint

from backend.config import get_settings
from backend.utils.logging import configure_logging, get_logger

app = typer.Typer(
    name="scai-prospectops",
    help="SCAI ProspectOps - B2B Prospecting Pipeline",
    add_completion=False,
)
console = Console()
logger = get_logger("cli")


def _setup():
    settings = get_settings()
    configure_logging(settings.log_dir_abs)


# ---------------------------------------------------------------------------
# fini command
# ---------------------------------------------------------------------------

@app.command()
def fini(
    companies: str = typer.Option(..., help="Comma-separated company names"),
    sdr: str = typer.Option("", help="SDR to assign"),
    region: str = typer.Option("", help="Region/market e.g. India, LATAM, Southeast Asia"),
    submit_n8n: bool = typer.Option(False, "--submit-n8n", help="Submit to n8n after sheet write"),
    thread_id: Optional[str] = typer.Option(None, help="Resume from existing thread ID"),
):
    """
    Run Fini (Target Builder) for a list of companies.
    Pauses at each company for operator confirmation before writing to sheet.
    """
    _setup()
    asyncio.run(_run_fini(companies, sdr, region, submit_n8n, thread_id))


async def _run_fini(companies_str: str, sdr: str, region: str, submit_n8n: bool, thread_id: str | None):
    from backend.agents.fini import build_fini_graph
    from backend.state import FiniState, TargetCompany

    company_names = [c.strip() for c in companies_str.split(",") if c.strip()]
    target_companies = [
        TargetCompany(raw_name=name, sdr_assigned=sdr or None)
        for name in company_names
    ]

    state = FiniState(companies=target_companies, submit_to_n8n=submit_n8n, region=region)
    thread_id = thread_id or str(uuid.uuid4())
    config = {"configurable": {"thread_id": thread_id}}

    app_graph = await build_fini_graph()

    console.print(Panel(
        f"[bold green]Starting Fini Agent[/bold green]\n"
        f"Companies: {', '.join(company_names)}\n"
        f"SDR: {sdr or 'Unassigned'}\n"
        f"Submit n8n: {submit_n8n}\n"
        f"Thread ID: [dim]{thread_id}[/dim]",
        title="SCAI ProspectOps - Fini",
        border_style="green",
    ))

    while True:
        # Run until interrupt or completion
        result = await app_graph.ainvoke(state, config)

        if isinstance(result, dict):
            state = FiniState(**result)
        else:
            state = result

        if state.status == "completed":
            console.print(Panel(
                f"[bold green]✓ Fini completed![/bold green]\n"
                f"Processed: {len(state.companies)} companies\n"
                f"Errors: {len(state.errors)}",
                border_style="green",
            ))
            if state.errors:
                for err in state.errors:
                    console.print(f"  [red]✗ {err}[/red]")
            break

        if state.status == "awaiting_confirmation":
            company = state.companies[state.current_index]
            _show_confirmation_prompt(company)

            choice = console.input("\n[bold]Confirm? ([green]y[/green]/[red]n[/red]/[yellow]edit[/yellow]): [/bold]").strip().lower()

            if choice == "y":
                await app_graph.aupdate_state(
                    config,
                    {"companies": [c.model_dump() for c in state.companies],
                     "operator_confirmed": True},
                )
                # Resume
                state = FiniState(**await app_graph.ainvoke(None, config))

            elif choice == "n":
                console.print("[yellow]Skipping this company.[/yellow]")
                companies = list(state.companies)
                companies[state.current_index] = companies[state.current_index].model_copy(
                    update={"operator_confirmed": False}
                )
                next_index = state.current_index + 1
                if next_index >= len(state.companies):
                    break
                state = state.model_copy(update={"current_index": next_index, "status": "running"})
                await app_graph.aupdate_state(config, state.model_dump())

            elif choice == "edit":
                state = await _handle_edit(state, app_graph, config)

            else:
                console.print("[red]Invalid input. Use y/n/edit.[/red]")

        else:
            break


def _show_confirmation_prompt(company):
    """Display company enrichment for operator review."""
    table = Table(title=f"Review: {company.raw_name}", show_header=True)
    table.add_column("Field", style="bold")
    table.add_column("Value")

    table.add_row("Raw Name", company.raw_name)
    table.add_row("Normalized Name", company.normalized_name or "[dim]not found[/dim]")
    table.add_row("Domain", company.domain or "[dim]not found[/dim]")
    table.add_row("Email Format", company.email_format or "[dim]not found[/dim]")
    table.add_row("LinkedIn Org ID", company.linkedin_org_id or "[dim]not found[/dim]")
    table.add_row("Sales Nav URL", company.sales_nav_url or "[dim]not found[/dim]")
    table.add_row("SDR Assigned", company.sdr_assigned or "[dim]unassigned[/dim]")
    table.add_row("Account Type (region)", company.account_type or "[dim]not inferred[/dim]")
    table.add_row("Account Size", company.account_size or "[dim]not inferred[/dim]")

    console.print(table)


async def _handle_edit(state, app_graph, config):
    """Let operator manually edit company fields."""
    company = state.companies[state.current_index]

    console.print("[yellow]Editing company fields. Press Enter to keep existing value.[/yellow]")

    normalized = console.input(f"Normalized name [{company.normalized_name}]: ").strip() or company.normalized_name
    domain = console.input(f"Domain [{company.domain}]: ").strip() or company.domain
    email_format = console.input(f"Email format [{company.email_format}]: ").strip() or company.email_format
    sdr = console.input(f"SDR [{company.sdr_assigned}]: ").strip() or company.sdr_assigned
    account_type = console.input(f"Account type/region [{company.account_type}]: ").strip() or company.account_type
    account_size = console.input(f"Account size (Large/Medium/Small) [{company.account_size}]: ").strip() or company.account_size

    updated = company.model_copy(update={
        "normalized_name": normalized,
        "domain": domain,
        "email_format": email_format,
        "sdr_assigned": sdr,
        "account_type": account_type,
        "account_size": account_size,
        "operator_confirmed": True,
    })
    companies = list(state.companies)
    companies[state.current_index] = updated
    new_state = state.model_copy(update={"companies": companies, "status": "running"})
    await app_graph.aupdate_state(config, new_state.model_dump())
    return new_state


# ---------------------------------------------------------------------------
# searcher command
# ---------------------------------------------------------------------------

@app.command()
def searcher(
    companies: str = typer.Option(..., help="Comma-separated company names (must already exist in Target Accounts)"),
    dm_roles: str = typer.Option(
        "VP Ecommerce,CDO,Head of Digital,CTO,CMO,VP Marketing,VP Sales",
        help="Comma-separated DM roles to find",
    ),
    thread_id: Optional[str] = typer.Option(None, help="Resume from existing thread ID"),
):
    """
    Run Searcher (Contact Discovery Agent).
    Reads org_id + domain + email_format from Target Accounts (Fini output),
    finds new contacts not already in First Clean List or First Clean List,
    and writes them to Searcher Output for Veri to process.
    """
    _setup()
    asyncio.run(_run_searcher(companies, dm_roles, thread_id))


async def _run_searcher(companies_str: str, dm_roles_str: str, thread_id: str | None):
    from backend.agents.searcher import build_searcher_graph
    from backend.state import SearcherState

    # Just company names — domain/org_id/email_format come from Target Accounts
    target_companies = [{"name": c.strip()} for c in companies_str.split(",") if c.strip()]

    dm_roles = [r.strip() for r in dm_roles_str.split(",") if r.strip()]
    first = target_companies[0] if target_companies else {"name": ""}

    state = SearcherState(
        target_company=first["name"],
        target_companies=target_companies,
        dm_roles=dm_roles,
    )
    thread_id = thread_id or str(uuid.uuid4())
    config = {"configurable": {"thread_id": thread_id}}

    console.print(Panel(
        f"[bold blue]Starting Searcher Agent[/bold blue]\n"
        f"Companies: {', '.join(c['name'] for c in target_companies)}\n"
        f"DM roles to find: {', '.join(dm_roles)}\n"
        f"(org_id + domain + email_format loaded from Target Accounts)\n"
        f"Thread ID: [dim]{thread_id}[/dim]",
        title="SCAI ProspectOps - Searcher",
        border_style="blue",
    ))

    app_graph = await build_searcher_graph()

    with console.status("[blue]Gap-filling contacts...[/blue]", spinner="dots"):
        result = await app_graph.ainvoke(state, config)

    if isinstance(result, dict):
        state = SearcherState(**result)
    else:
        state = result

    console.print(Panel(
        f"[bold blue]✓ Searcher completed![/bold blue]\n"
        f"Contacts appended: {len(state.discovered_contacts)}\n"
        f"Errors: {len(state.errors)}",
        border_style="blue",
    ))

    if state.discovered_contacts:
        table = Table(title="Appended Contacts")
        table.add_column("Name")
        table.add_column("Role")
        table.add_column("Bucket")
        table.add_column("Email")
        table.add_column("LinkedIn")

        for c in state.discovered_contacts[:20]:
            table.add_row(
                c.full_name,
                (c.role_title or "")[:40],
                c.role_bucket,
                c.email or "-",
                "✓" if c.linkedin_verified else "✗",
            )
        console.print(table)


# ---------------------------------------------------------------------------
# veri command
# ---------------------------------------------------------------------------

@app.command()
def veri(
    thread_id: Optional[str] = typer.Option(None, help="Resume from existing thread ID"),
):
    """Run Veri (Contact QC) on all pending contacts in First Clean List."""
    _setup()
    asyncio.run(_run_veri(thread_id))


async def _run_veri(thread_id: str | None):
    from backend.agents.veri import build_veri_graph
    from backend.state import VeriState

    state = VeriState(contacts=[])
    thread_id = thread_id or str(uuid.uuid4())
    config = {"configurable": {"thread_id": thread_id}}

    console.print(Panel(
        f"[bold magenta]Starting Veri Agent[/bold magenta]\n"
        f"Thread ID: [dim]{thread_id}[/dim]",
        title="SCAI ProspectOps - Veri",
        border_style="magenta",
    ))

    app_graph = await build_veri_graph()

    with console.status("[magenta]Verifying contacts...[/magenta]", spinner="dots"):
        result = await app_graph.ainvoke(state, {**config, "recursion_limit": 150_000})

    if isinstance(result, dict):
        state = VeriState(**result)
    else:
        state = result

    console.print(Panel(
        f"[bold magenta]✓ Veri completed![/bold magenta]\n"
        f"[green]VERIFIED: {state.verified_count}[/green]\n"
        f"[yellow]REVIEW: {state.review_count}[/yellow]\n"
        f"[red]REJECT: {state.rejected_count}[/red]\n"
        f"Errors: {len(state.errors)}",
        border_style="magenta",
    ))


# ---------------------------------------------------------------------------
# status command
# ---------------------------------------------------------------------------

@app.command()
def status(
    thread_id: str = typer.Argument(..., help="Thread ID to check"),
):
    """Check pipeline status for a thread ID."""
    _setup()

    settings = get_settings()
    try:
        import sqlite3
        from langgraph.checkpoint.sqlite import SqliteSaver
        conn = sqlite3.connect(settings.checkpoint_db_abs, check_same_thread=False)
        checkpointer = SqliteSaver(conn)
        config = {"configurable": {"thread_id": thread_id}}
        checkpoint = checkpointer.get(config)

        if not checkpoint:
            console.print(f"[red]No checkpoint found for thread ID: {thread_id}[/red]")
            return

        console.print(Panel(
            f"Thread ID: {thread_id}\n"
            f"Checkpoint exists: [green]Yes[/green]",
            title="Pipeline Status",
            border_style="cyan",
        ))

    except Exception as e:
        console.print(f"[red]Error checking status: {e}[/red]")


# ---------------------------------------------------------------------------
# resume command
# ---------------------------------------------------------------------------

@app.command()
def resume(
    thread_id: str = typer.Argument(..., help="Thread ID to resume"),
    agent: str = typer.Option("fini", help="Agent to resume: fini, searcher, veri"),
):
    """Resume a paused pipeline from a checkpoint."""
    _setup()
    asyncio.run(_run_resume(thread_id, agent))


async def _run_resume(thread_id: str, agent: str):
    config = {"configurable": {"thread_id": thread_id}}

    console.print(f"[cyan]Resuming {agent} agent from thread {thread_id}...[/cyan]")

    if agent == "fini":
        from backend.agents.fini import build_fini_graph
        app_graph = await build_fini_graph()
        result = await app_graph.ainvoke(None, config)
    elif agent == "searcher":
        from backend.agents.searcher import build_searcher_graph
        app_graph = await build_searcher_graph()
        result = await app_graph.ainvoke(None, config)
    elif agent == "veri":
        from backend.agents.veri import build_veri_graph
        app_graph = await build_veri_graph()
        result = await app_graph.ainvoke(None, config)
    else:
        console.print(f"[red]Unknown agent: {agent}[/red]")
        return

    console.print(f"[green]Resume completed for {agent}.[/green]")


# ---------------------------------------------------------------------------
# ui command
# ---------------------------------------------------------------------------

@app.command()
def ui():
    """Start the web UI server (non-tech friendly interface)."""
    _setup()
    settings = get_settings()

    console.print(Panel(
        f"[bold cyan]Starting SCAI ProspectOps Web UI[/bold cyan]\n"
        f"Open your browser at: [link]http://localhost:{settings.ui_port}[/link]\n"
        f"Host: {settings.ui_host}:{settings.ui_port}",
        title="Web UI",
        border_style="cyan",
    ))

    import os
    import uvicorn
    from backend.api import create_app
    # Railway/Render inject PORT dynamically; fall back to settings.ui_port for local dev
    port = int(os.environ.get("PORT", settings.ui_port))
    uvicorn.run(
        create_app(),
        host=settings.ui_host,
        port=port,
        reload=False,
    )


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    app()

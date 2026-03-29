'use client';

import React, { useEffect, useState } from 'react';
import Link from 'next/link';
import { usePathname } from 'next/navigation';
import { apiUrl } from '@/lib/api';

const NAV_ITEMS = [
  { id: 'dashboard', href: '/', label: 'Overview', sub: null },
  { id: 'fini', href: '/fini', label: 'Fini', sub: 'Target Builder' },
  { id: 'searcher', href: '/searcher', label: 'Searcher', sub: 'Contact Discovery' },
  { id: 'veri', href: '/veri', label: 'Veri', sub: 'Contact QC' },
  { id: 'settings', href: '/settings', label: 'Settings', sub: null },
];

export default function Sidebar() {
  const pathname = usePathname();
  const [configStatus, setConfigStatus] = useState<Record<string, boolean> | null>(null);

  useEffect(() => {
    fetch(apiUrl('/api/config/check'))
      .then((r) => r.json())
      .then(setConfigStatus)
      .catch(() => null);
  }, []);

  const isActive = (href: string) => {
    if (href === '/' && pathname === '/') return true;
    if (href !== '/' && pathname.startsWith(href)) return true;
    return false;
  };

  return (
    <aside className="relative w-60 flex flex-col border-r border-white/[0.06] bg-black/40 backdrop-blur-2xl">

      {/* Brand */}
      <div className="px-6 pt-7 pb-6 border-b border-white/[0.05]">
        <Link href="/" className="block">
          <img
            src="/saleslogo.png"
            alt="SalesCode.ai"
            className="h-8 w-auto object-contain object-left opacity-90 hover:opacity-100 transition-opacity"
          />
        </Link>
      </div>

      {/* Navigation */}
      <nav className="flex-1 px-3 py-4 space-y-0.5">
        <div className="px-3 mb-3 text-[9px] font-bold text-white/50 uppercase tracking-[0.4em]">Navigation</div>
        {NAV_ITEMS.map((item) => {
          const active = isActive(item.href);
          return (
            <Link
              key={item.id}
              href={item.href}
              className={`w-full flex items-center gap-3 px-3 py-2.5 rounded-xl transition-all duration-200 group ${
                active
                  ? 'bg-white/[0.07] text-white'
                  : 'text-white/55 hover:text-white/85 hover:bg-white/[0.04]'
              }`}
            >
              <div className={`w-1.5 h-1.5 rounded-full flex-shrink-0 transition-all duration-200 ${
                active ? 'bg-white/70' : 'bg-white/40 group-hover:bg-white/50'
              }`} />
              <div>
                <div className={`text-[13px] font-semibold leading-none ${active ? 'text-white' : ''}`}>
                  {item.label}
                </div>
                {item.sub && (
                  <div className={`text-[10px] mt-0.5 leading-none ${active ? 'text-white/75' : 'text-white/50'}`}>
                    {item.sub}
                  </div>
                )}
              </div>
            </Link>
          );
        })}
      </nav>

      {/* System Health */}
      {configStatus && (
        <div className="px-5 py-4 border-t border-white/[0.05]">
          <div className="flex items-center justify-between mb-3">
            <span className="text-[9px] font-bold text-white/50 uppercase tracking-[0.35em]">System Health</span>
            <div className="w-1.5 h-1.5 rounded-full bg-white/40 animate-pulse" />
          </div>
          <div className="grid grid-cols-2 gap-x-3 gap-y-2">
            {Object.entries(configStatus).slice(0, 4).map(([key, ok]) => (
              <div key={key} className="flex items-center gap-1.5 min-w-0">
                <div className={`w-1 h-1 rounded-full flex-shrink-0 ${ok ? 'bg-white/40' : 'bg-red-500/60'}`} />
                <span className="text-[9px] font-mono text-white/55 truncate uppercase tracking-wider">
                  {key.split('_')[0]}
                </span>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Footer */}
      <div className="px-5 py-3 border-t border-white/[0.04]">
        <span className="text-[9px] font-mono text-white/10">© 2026 SalesCode.ai · PRO</span>
      </div>
    </aside>
  );
}

/**
 * API utility for SCAI ProspectOps (Next.js edition).
 *
 * All /api/* and /ws/* paths are transparently proxied to the FastAPI backend
 * via next.config.ts rewrites, so we can always use relative paths on the
 * client side without any CORS issues.
 */

/** Build a full API URL. On the client we use relative paths (proxied). */
export function apiUrl(path: string): string {
  const cleanPath = path.startsWith('/') ? path : `/${path}`;
  // Server-side: must use absolute URL to the backend
  if (typeof window === 'undefined') {
    const base = process.env.NEXT_PUBLIC_API_BASE_URL ?? 'http://localhost:8080';
    return `${base}${cleanPath}`;
  }
  // Client-side: always relative — Next.js rewrites handle forwarding
  return cleanPath;
}

export function wsUrl(threadId: string): string {
  const base = process.env.NEXT_PUBLIC_API_BASE_URL ?? 'http://localhost:8080';
  const wsBase = base.replace(/^http/, 'ws');
  return `${wsBase}/ws/${threadId}`;
}

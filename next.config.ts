import type { NextConfig } from "next";

// Server-side: internal Docker URL for API proxy rewrites
const BACKEND = process.env.BACKEND_URL ?? "http://localhost:8080";

// Client-side: public URL baked into the JS bundle for WebSocket connections.
// On AWS/Docker: set NEXT_PUBLIC_API_BASE_URL to your EC2 IP or domain at build time.
// Falls back to BACKEND (works for local dev where both are the same).
const PUBLIC_API_BASE = process.env.NEXT_PUBLIC_API_BASE_URL ?? BACKEND;

const nextConfig: NextConfig = {
  // Standalone output — required for the optimised Docker image
  output: "standalone",

  // Proxy /api/* and /ws/* to the FastAPI backend — no CORS issues on any host
  async rewrites() {
    return [
      { source: "/api/:path*", destination: `${BACKEND}/api/:path*` },
      { source: "/ws/:path*",  destination: `${BACKEND}/ws/:path*`  },
    ];
  },

  // Bake the public backend URL into the client bundle (used by wsUrl() for WebSockets)
  env: {
    NEXT_PUBLIC_API_BASE_URL: PUBLIC_API_BASE,
  },
};

export default nextConfig;

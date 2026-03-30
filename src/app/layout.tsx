import type { Metadata } from "next";
import { Inter, JetBrains_Mono } from "next/font/google";
import Sidebar from "@/components/Sidebar";
import "./globals.css";

const inter = Inter({
  variable: "--font-inter",
  subsets: ["latin"],
});

const jetbrainsMono = JetBrains_Mono({
  variable: "--font-jetbrains-mono",
  subsets: ["latin"],
});

export const metadata: Metadata = {
  title: "SCAI ProspectOps | Enterprise",
  description: "Next-generation B2B prospecting pipeline",
  icons: {
    icon: "/favicon.svg",
    shortcut: "/favicon.svg",
  },
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en" className="h-full">
      <body
        className={`${inter.variable} ${jetbrainsMono.variable} antialiased h-screen overflow-hidden selection:bg-blue-500/30 flex bg-[#080a12] text-[#f1f5f9] font-sans`}
      >
        <div className="bg-ambient" />

        <Sidebar />

        <main className="flex-1 overflow-y-auto no-scrollbar relative">
          {/* Dynamic Scan Line (Subtle) */}
          <div className="fixed top-0 left-0 right-0 h-[1px] bg-gradient-to-r from-transparent via-blue-500/10 to-transparent pointer-events-none z-50 animate-[scan_10s_linear_infinite]" />

          <div className="page-transition min-h-full pb-20">
            {children}
          </div>

          {/* Floating Deployment Badge */}
          <div className="fixed bottom-6 right-8 z-50">
            <div className="flex items-center gap-2 px-3 py-1.5 bg-slate-900/80 border border-white/10 rounded-full backdrop-blur-md shadow-lg shadow-black/40">
              <span className="relative flex h-2 w-2">
                <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-emerald-400 opacity-75"></span>
                <span className="relative inline-flex rounded-full h-2 w-2 bg-emerald-500"></span>
              </span>
              <span className="text-[9px] font-bold text-slate-400 uppercase tracking-widest tabular-nums">
                Prod Alpha 2.3
              </span>
            </div>
          </div>
        </main>
      </body>
    </html>
  );
}

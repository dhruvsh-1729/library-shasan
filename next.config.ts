import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  /* config options here */
  reactStrictMode: true,
  // Search is the home page; the library catalogue moved to /library.
  // Old /search links (with their ?q=… query, which Next carries over) keep working.
  async redirects() {
    return [{ source: "/search", destination: "/", permanent: false }];
  },
};

export default nextConfig;

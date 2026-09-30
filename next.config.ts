import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  /* config options here */
  reactStrictMode: true,
  // harfbuzzjs loads its .wasm next to its own module file; bundling breaks that path.
  serverExternalPackages: ["harfbuzzjs"],
  // Search is the home page; the library catalogue moved to /library.
  // Old /search links (with their ?q=… query, which Next carries over) keep working.
  async redirects() {
    return [{ source: "/search", destination: "/", permanent: false }];
  },
};

export default nextConfig;

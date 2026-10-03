import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  /* config options here */
  reactStrictMode: true,
  // harfbuzzjs loads its .wasm next to its own module file; bundling breaks that path.
  serverExternalPackages: ["harfbuzzjs"],
  // Granth covers are full-size uploads (up to 600 KB); /library shows them
  // through the image optimizer as small WebP thumbnails instead. An upload's
  // URL never changes content, so a resized copy can be kept for a month.
  images: {
    remotePatterns: [
      { protocol: "https", hostname: "*.ufs.sh" },
      { protocol: "https", hostname: "utfs.io" },
    ],
    formats: ["image/webp"],
    qualities: [60],
    minimumCacheTTL: 60 * 60 * 24 * 30,
  },
  // Search is the home page; the library catalogue moved to /library.
  // Old /search links (with their ?q=… query, which Next carries over) keep working.
  async redirects() {
    return [{ source: "/search", destination: "/", permanent: false }];
  },
};

export default nextConfig;

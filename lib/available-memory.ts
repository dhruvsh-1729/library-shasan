import { readFile } from "node:fs/promises";

async function readNumberFile(path: string) {
  try {
    const raw = (await readFile(path, "utf8")).trim();
    if (!raw || raw === "max") return null;
    const value = Number(raw);
    return Number.isFinite(value) ? value : null;
  } catch {
    return null;
  }
}

// Inside a container `/proc/meminfo` reports the host's memory, not the memory
// this process is actually allowed to use, so the cgroup limit is checked first.
async function cgroupAvailableMB() {
  const pairs: Array<[string, string]> = [
    ["/sys/fs/cgroup/memory.max", "/sys/fs/cgroup/memory.current"],
    [
      "/sys/fs/cgroup/memory/memory.limit_in_bytes",
      "/sys/fs/cgroup/memory/memory.usage_in_bytes",
    ],
  ];

  for (const [limitPath, usagePath] of pairs) {
    const limit = await readNumberFile(limitPath);
    const usage = await readNumberFile(usagePath);
    if (limit == null || usage == null) continue;
    // Unconstrained cgroups report a sentinel near the top of the 64-bit range.
    if (limit >= Number.MAX_SAFE_INTEGER || limit > 1024 ** 4) continue;
    return Math.max(0, (limit - usage) / (1024 * 1024));
  }

  return null;
}

async function hostAvailableMB() {
  try {
    const meminfo = await readFile("/proc/meminfo", "utf8");
    const match = meminfo.match(/^MemAvailable:\s+(\d+)\s+kB/m);
    if (!match) return null;
    return Number(match[1]) / 1024;
  } catch {
    return null;
  }
}

export async function availableMemoryMB() {
  const cgroup = await cgroupAvailableMB();
  if (cgroup != null) return cgroup;
  return hostAvailableMB();
}

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

// The cgroup's usage counts the kernel's cache of files read or written (every
// PDF kept in the source cache lands there). The kernel drops that cache the
// moment a process needs the memory, so it is free memory, not used memory.
async function fileCacheBytes(statPath: string) {
  try {
    const stat = await readFile(statPath, "utf8");
    let bytes = 0;
    for (const key of ["inactive_file", "active_file"]) {
      const match = stat.match(new RegExp(`^(?:total_)?${key} (\\d+)$`, "m"));
      if (match) bytes += Number(match[1]);
    }
    return bytes;
  } catch {
    return 0;
  }
}

// Inside a container `/proc/meminfo` reports the host's memory, not the memory
// this process is actually allowed to use, so the cgroup limit is checked first.
async function cgroupAvailableMB() {
  const sets: Array<[string, string, string]> = [
    ["/sys/fs/cgroup/memory.max", "/sys/fs/cgroup/memory.current", "/sys/fs/cgroup/memory.stat"],
    [
      "/sys/fs/cgroup/memory/memory.limit_in_bytes",
      "/sys/fs/cgroup/memory/memory.usage_in_bytes",
      "/sys/fs/cgroup/memory/memory.stat",
    ],
  ];

  for (const [limitPath, usagePath, statPath] of sets) {
    const limit = await readNumberFile(limitPath);
    const usage = await readNumberFile(usagePath);
    if (limit == null || usage == null) continue;
    // Unconstrained cgroups report a sentinel near the top of the 64-bit range.
    if (limit >= Number.MAX_SAFE_INTEGER || limit > 1024 ** 4) continue;
    const used = Math.max(0, usage - (await fileCacheBytes(statPath)));
    return Math.max(0, (limit - used) / (1024 * 1024));
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

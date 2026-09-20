import type { UserStatus } from "@/lib/auth-permissions";

declare module "next-auth" {
  interface Session {
    user: {
      id: string;
      email: string;
      name: string | null;
      image: string | null;
      role: string;
      roleLabel: string;
      status: UserStatus;
      permissions: string[];
    };
  }
}

declare module "next-auth/jwt" {
  interface JWT {
    userId?: string;
    role?: string;
    roleLabel?: string;
    status?: UserStatus;
    permissions?: string[];
    syncedAt?: number;
  }
}

export {};

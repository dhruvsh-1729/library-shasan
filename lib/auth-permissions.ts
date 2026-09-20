export const PERMISSIONS = {
  libraryRead: "library.read",
  ocrWrite: "ocr.write",
  pdfBuild: "pdf.build",
  usersManage: "users.manage",
  rolesManage: "roles.manage",
} as const;

export type Permission = (typeof PERMISSIONS)[keyof typeof PERMISSIONS];

export const ALL_PERMISSIONS: Permission[] = Object.values(PERMISSIONS);

export const PERMISSION_LABELS: Record<Permission, string> = {
  "library.read": "Read library and search",
  "ocr.write": "Apply and revert OCR corrections",
  "pdf.build": "Build and download PDFs",
  "users.manage": "Approve signups and manage users",
  "roles.manage": "Create roles and assign them",
};

export const SUPER_ADMIN_ROLE = "super_admin";

export const USER_STATUSES = ["pending", "approved", "rejected"] as const;
export type UserStatus = (typeof USER_STATUSES)[number];

export function isPermission(value: unknown): value is Permission {
  return typeof value === "string" && (ALL_PERMISSIONS as string[]).includes(value);
}

export function hasPermission(permissions: readonly string[] | undefined, required: Permission) {
  return Array.isArray(permissions) && permissions.includes(required);
}

/** Role names are used as URL-safe identifiers and as the app_users.role foreign key. */
export function normalizeRoleName(value: unknown) {
  return String(value ?? "")
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9_]+/g, "_")
    .replace(/^_+|_+$/g, "")
    .slice(0, 40);
}

export function normalizeEmail(value: unknown) {
  return String(value ?? "").trim().toLowerCase();
}

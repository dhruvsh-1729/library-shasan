import "@/styles/globals.css";
import "@/styles/auth.css";
import "@/styles/admin.css";
import "@/styles/search.css";
import "@/styles/theme.css";
import "@/styles/ask.css";
import type { AppProps } from "next/app";
import { Archivo, Noto_Serif_Gujarati, Tiro_Devanagari_Sanskrit } from "next/font/google";
import { SessionProvider } from "next-auth/react";
import type { Session } from "next-auth";

// One set of faces for every page: Archivo for the interface, and faces made
// for the scripts the granths are written in.
const uiFont = Archivo({ subsets: ["latin"], axes: ["wdth"], variable: "--lt-font-ui", display: "swap" });
const devanagariFont = Tiro_Devanagari_Sanskrit({
  subsets: ["devanagari", "latin"],
  weight: "400",
  variable: "--lt-font-deva",
  display: "swap",
});
const gujaratiFont = Noto_Serif_Gujarati({ subsets: ["gujarati"], variable: "--lt-font-guj", display: "swap" });

export default function App({
  Component,
  pageProps: { session, ...pageProps },
}: AppProps<{ session: Session | null }>) {
  return (
    <SessionProvider session={session}>
      <div className={`ltFonts ${uiFont.variable} ${devanagariFont.variable} ${gujaratiFont.variable}`}>
        <Component {...pageProps} />
      </div>
    </SessionProvider>
  );
}

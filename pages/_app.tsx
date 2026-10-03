import "@/styles/fonts.css";
import "@/styles/globals.css";
import "@/styles/auth.css";
import "@/styles/admin.css";
import "@/styles/search.css";
import "@/styles/theme.css";
import "@/styles/ask.css";
import "@/styles/vyutpatti.css";
import "@/styles/extract.css";
import "@/styles/sheet.css";
import "@/styles/phone.css";
import "@/styles/appnav.css";
import type { AppProps } from "next/app";
import { Archivo } from "next/font/google";
import { SessionProvider } from "next-auth/react";
import type { Session } from "next-auth";
import { useEffect } from "react";

// One set of faces for every page: Archivo for the interface, and faces made
// for the scripts the granths are written in. Devanagari is Noto Serif, not a
// traditional face: Tiro Devanagari Sanskrit stacks क्त so it reads as त्त
// (शक्ति as शत्ति) and क्त्र as क्र, and a reader checking hits cannot tell.
const uiFont = Archivo({ subsets: ["latin"], axes: ["wdth"], variable: "--lt-font-ui", display: "swap" });
// The Devanagari and Gujarati faces (Noto Serif, cut to weights 400-700 for
// slow connections) are self-hosted: styles/fonts.css, preloaded in _document.

export default function App({
  Component,
  pageProps: { session, ...pageProps },
}: AppProps<{ session: Session | null }>) {
  // The service worker (public/sw.js) keeps pages and lookups for slow or no internet; production only,
  // so development always sees fresh code.
  useEffect(() => {
    if (process.env.NODE_ENV !== "production" || !("serviceWorker" in navigator)) return;
    navigator.serviceWorker.register("/sw.js").catch(() => undefined);
  }, []);
  return (
    <SessionProvider session={session}>
      <div className={`ltFonts ${uiFont.variable}`}>
        <Component {...pageProps} />
      </div>
    </SessionProvider>
  );
}

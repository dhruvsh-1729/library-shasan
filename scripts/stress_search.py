#!/usr/bin/env python3
"""Load/stress test for the library search API.

Zero dependencies: stdlib urllib + a thread pool.

The endpoint under test is cached twice over (a 60s in-process memory cache in
lib/api-cache.ts, plus `s-maxage=60` on the Vercel CDN), so replaying one URL
measures the cache, not the backend. Pick the cache mode deliberately:

  --cache bust    unique `_cb` param per request -> always MISS, measures
                  Turso FTS + Supabase under real load (the honest worst case)
  --cache corpus  rotates a query corpus -> realistic mix of HIT and MISS
  --cache warm    replays one identical URL -> measures the cache/CDN path

Examples:
  # baseline: how fast is one uncached request
  ./scripts/stress_search.py --concurrency 1 --requests 20 --cache bust

  # steady load for 30s against the real backend
  ./scripts/stress_search.py --concurrency 10 --duration 30 --cache bust

  # ramp to find the knee, save raw samples
  ./scripts/stress_search.py --ramp 2,5,10,20,40 --stage-duration 20 \
      --cache corpus --csv /tmp/search-load.csv

  # sanity-check param handling before trusting the load numbers
  ./scripts/stress_search.py --probe
"""

from __future__ import annotations

import argparse
import csv
import gzip
import json
import math
import os
import random
import statistics
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import Any

DEFAULT_BASE_URL = "https://library-shasan.vercel.app"
DEFAULT_PATH = "/api/search"

# (roman, devanagari, gujarati) -- mirrors how pages/search.tsx sends q plus
# repeated queryVariant params.
DEFAULT_CORPUS: list[tuple[str, str, str]] = [
    ("aatmaa", "आत्मा", "આત્મા"),
    ("dharma", "धर्म", "ધર્મ"),
    ("karma", "कर्म", "કર્મ"),
    ("moksha", "मोक्ष", "મોક્ષ"),
    ("gyaan", "ज्ञान", "જ્ઞાન"),
    ("jiva", "जीव", "જીવ"),
    ("tapa", "तप", "તપ"),
    ("shraddha", "श्रद्धा", "શ્રદ્ધા"),
    ("samyak", "सम्यक", "સમ્યક"),
    ("bhagwan", "भगवान", "ભગવાન"),
    ("muni", "मुनि", "મુનિ"),
    ("sutra", "सूत्र", "સૂત્ર"),
    ("charitra", "चरित्र", "ચરિત્ર"),
    ("puja", "पूजा", "પૂજા"),
    ("guru", "गुरु", "ગુરુ"),
    ("shaastra", "शास्त्र", "શાસ્ત્ર"),
]

MATCH_MODES = ("exact_word", "contains", "begins_with", "ends_with")


@dataclass
class Sample:
    """One request outcome."""

    stage: str
    started_at: float
    ttfb_ms: float
    total_ms: float
    status: int
    ok: bool
    bytes_read: int
    api_cache: str = ""
    cdn_cache: str = ""
    total_results: int = -1
    returned_results: int = -1
    per_page: int = -1
    error: str = ""
    query: str = ""


@dataclass
class StageResult:
    name: str
    concurrency: int
    wall_seconds: float
    samples: list[Sample] = field(default_factory=list)


class Collector:
    """Thread-safe sample sink with a live progress counter."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.samples: list[Sample] = []
        self.sent = 0
        self.failed = 0

    def add(self, sample: Sample) -> None:
        with self._lock:
            self.samples.append(sample)
            self.sent += 1
            if not sample.ok:
                self.failed += 1

    def snapshot(self) -> tuple[int, int]:
        with self._lock:
            return self.sent, self.failed


def build_url(args: argparse.Namespace, entry: tuple[str, str, str], nonce: str | None) -> str:
    roman, devanagari, gujarati = entry
    params: list[tuple[str, str]] = [("q", roman)]
    if not args.no_variants:
        params.append(("queryVariant", devanagari))
        params.append(("queryVariant", gujarati))
    params.append(("limit", str(args.limit)))
    params.append(("page", str(args.page)))
    params.append(("matchMode", args.match_mode))
    if args.granths:
        params.append(("granths", args.granths))
    if nonce is not None:
        # Any unrecognized param still lands in buildCacheKey() and in the CDN
        # cache key, so this forces a cold path without changing the query.
        params.append(("_cb", nonce))
    query = urllib.parse.urlencode(params, encoding="utf-8")
    return f"{args.base_url.rstrip('/')}{args.path}?{query}"


def pick_entry(args: argparse.Namespace, corpus: list[tuple[str, str, str]], index: int) -> tuple[str, str, str]:
    if args.cache == "warm":
        return corpus[0]
    if args.shuffle:
        return random.choice(corpus)
    return corpus[index % len(corpus)]


def do_request(url: str, args: argparse.Namespace, stage: str, query: str) -> Sample:
    headers = {
        "User-Agent": args.user_agent,
        "Accept": "application/json",
        "Accept-Encoding": "gzip" if args.gzip else "identity",
    }
    request = urllib.request.Request(url, headers=headers, method="GET")

    started_at = time.time()
    t0 = time.perf_counter()
    status = 0
    ok = False
    error = ""
    body = b""
    api_cache = ""
    cdn_cache = ""
    ttfb_ms = float("nan")

    try:
        response = urllib.request.urlopen(request, timeout=args.timeout)
        ttfb_ms = (time.perf_counter() - t0) * 1000.0
        with response:
            status = response.status
            api_cache = response.headers.get("X-Library-Api-Cache", "") or ""
            cdn_cache = response.headers.get("x-vercel-cache", "") or ""
            body = response.read()
            if args.gzip and (response.headers.get("Content-Encoding", "").lower() == "gzip"):
                body = gzip.decompress(body)
        ok = 200 <= status < 300
    except urllib.error.HTTPError as exc:
        ttfb_ms = (time.perf_counter() - t0) * 1000.0
        status = exc.code
        api_cache = exc.headers.get("X-Library-Api-Cache", "") or ""
        cdn_cache = exc.headers.get("x-vercel-cache", "") or ""
        try:
            body = exc.read()
        except Exception:  # noqa: BLE001 - body is best-effort on an error path
            body = b""
        error = f"HTTP {exc.code}"
    except urllib.error.URLError as exc:
        error = f"{type(exc.reason).__name__ if exc.reason is not None else 'URLError'}: {exc.reason}"
    except Exception as exc:  # noqa: BLE001 - never let one request kill the run
        error = f"{type(exc).__name__}: {exc}"

    total_ms = (time.perf_counter() - t0) * 1000.0

    sample = Sample(
        stage=stage,
        started_at=started_at,
        ttfb_ms=ttfb_ms,
        total_ms=total_ms,
        status=status,
        ok=ok,
        bytes_read=len(body),
        api_cache=api_cache,
        cdn_cache=cdn_cache,
        error=error,
        query=query,
    )

    if body and not args.no_parse:
        try:
            payload = json.loads(body)
        except (ValueError, UnicodeDecodeError):
            if ok:
                sample.ok = False
                sample.error = "invalid JSON body"
        else:
            if isinstance(payload, dict):
                total = payload.get("total")
                if isinstance(total, int):
                    sample.total_results = total
                results = payload.get("results")
                if isinstance(results, list):
                    sample.returned_results = len(results)
                per_page = payload.get("per_page")
                if isinstance(per_page, int):
                    sample.per_page = per_page
                if ok and "error" in payload:
                    sample.ok = False
                    sample.error = f"payload error: {payload['error']}"
                if not ok and isinstance(payload.get("error"), str):
                    sample.error = f"HTTP {status}: {payload['error']}"

    return sample


def run_stage(
    args: argparse.Namespace,
    corpus: list[tuple[str, str, str]],
    stage: str,
    concurrency: int,
    duration: float | None,
    request_budget: int | None,
) -> StageResult:
    collector = Collector()
    deadline = (time.perf_counter() + duration) if duration else None
    counter = {"issued": 0}
    counter_lock = threading.Lock()
    stop = threading.Event()

    def next_index() -> int | None:
        with counter_lock:
            if request_budget is not None and counter["issued"] >= request_budget:
                return None
            index = counter["issued"]
            counter["issued"] = index + 1
            return index

    def worker() -> None:
        while not stop.is_set():
            if deadline is not None and time.perf_counter() >= deadline:
                return
            index = next_index()
            if index is None:
                return
            entry = pick_entry(args, corpus, index)
            nonce = None
            if args.cache == "bust":
                nonce = f"{os.getpid()}-{stage}-{index}-{int(time.time() * 1000)}"
            url = build_url(args, entry, nonce)
            collector.add(do_request(url, args, stage, entry[0]))
            if args.think > 0:
                time.sleep(args.think)

    stage_started = time.perf_counter()
    progress_stop = threading.Event()

    def progress() -> None:
        while not progress_stop.wait(1.0):
            sent, failed = collector.snapshot()
            elapsed = time.perf_counter() - stage_started
            rate = sent / elapsed if elapsed > 0 else 0.0
            sys.stderr.write(
                f"\r  [{stage}] c={concurrency} sent={sent} failed={failed} "
                f"{rate:6.1f} req/s  {elapsed:5.1f}s"
            )
            sys.stderr.flush()

    progress_thread = threading.Thread(target=progress, daemon=True)
    if not args.quiet:
        progress_thread.start()

    try:
        with ThreadPoolExecutor(max_workers=concurrency) as pool:
            futures = [pool.submit(worker) for _ in range(concurrency)]
            for future in futures:
                future.result()
    except KeyboardInterrupt:
        stop.set()
        raise
    finally:
        progress_stop.set()
        if not args.quiet:
            progress_thread.join(timeout=1.5)
            sys.stderr.write("\r" + " " * 78 + "\r")
            sys.stderr.flush()

    return StageResult(
        name=stage,
        concurrency=concurrency,
        wall_seconds=time.perf_counter() - stage_started,
        samples=collector.samples,
    )


def percentile(values: list[float], pct: float) -> float:
    """Nearest-rank percentile."""
    if not values:
        return float("nan")
    ordered = sorted(values)
    rank = math.ceil(pct / 100.0 * len(ordered))
    return ordered[max(0, min(len(ordered) - 1, rank - 1))]


def summarize(stage: StageResult) -> dict[str, Any]:
    samples = stage.samples
    ok = [s for s in samples if s.ok]
    latencies = [s.total_ms for s in ok]
    ttfbs = [s.ttfb_ms for s in ok if not math.isnan(s.ttfb_ms)]

    summary: dict[str, Any] = {
        "stage": stage.name,
        "concurrency": stage.concurrency,
        "wall_seconds": round(stage.wall_seconds, 3),
        "requests": len(samples),
        "ok": len(ok),
        "failed": len(samples) - len(ok),
        "error_rate_pct": round(100.0 * (len(samples) - len(ok)) / len(samples), 2) if samples else 0.0,
        "throughput_rps": round(len(samples) / stage.wall_seconds, 2) if stage.wall_seconds > 0 else 0.0,
        "status_codes": dict(sorted(Counter(s.status for s in samples).items())),
        "errors": dict(Counter(s.error for s in samples if s.error).most_common(8)),
        "api_cache": dict(Counter(s.api_cache or "-" for s in samples)),
        "cdn_cache": dict(Counter(s.cdn_cache or "-" for s in samples)),
    }

    if latencies:
        summary["latency_ms"] = {
            "min": round(min(latencies), 1),
            "mean": round(statistics.fmean(latencies), 1),
            "p50": round(percentile(latencies, 50), 1),
            "p90": round(percentile(latencies, 90), 1),
            "p95": round(percentile(latencies, 95), 1),
            "p99": round(percentile(latencies, 99), 1),
            "max": round(max(latencies), 1),
        }
    if ttfbs:
        summary["ttfb_ms"] = {
            "p50": round(percentile(ttfbs, 50), 1),
            "p95": round(percentile(ttfbs, 95), 1),
            "max": round(max(ttfbs), 1),
        }
    if ok:
        summary["avg_bytes"] = int(statistics.fmean([s.bytes_read for s in ok]))
        totals = [s.total_results for s in ok if s.total_results >= 0]
        if totals:
            summary["total_results_seen"] = sorted(set(totals))[:5]
        empty = [s for s in ok if s.returned_results == 0]
        if empty:
            summary["empty_result_responses"] = len(empty)

    return summary


def print_summary(summary: dict[str, Any]) -> None:
    lat = summary.get("latency_ms", {})
    print(f"\n─── stage {summary['stage']} (concurrency {summary['concurrency']}) ───")
    print(
        f"  requests {summary['requests']}  ok {summary['ok']}  failed {summary['failed']}"
        f"  ({summary['error_rate_pct']}% errors)  {summary['throughput_rps']} req/s"
        f"  over {summary['wall_seconds']}s"
    )
    if lat:
        print(
            f"  latency ms   p50 {lat['p50']}   p90 {lat['p90']}   p95 {lat['p95']}"
            f"   p99 {lat['p99']}   max {lat['max']}   (mean {lat['mean']}, min {lat['min']})"
        )
    ttfb = summary.get("ttfb_ms")
    if ttfb:
        print(f"  ttfb ms      p50 {ttfb['p50']}   p95 {ttfb['p95']}   max {ttfb['max']}")
    print(f"  status       {summary['status_codes']}")
    print(f"  api cache    {summary['api_cache']}    cdn {summary['cdn_cache']}")
    if summary.get("avg_bytes") is not None:
        print(f"  avg body     {summary['avg_bytes']} bytes")
    if summary.get("empty_result_responses"):
        print(f"  ⚠ {summary['empty_result_responses']} ok responses had zero results")
    if summary.get("errors"):
        print("  error detail:")
        for message, count in summary["errors"].items():
            print(f"    {count:5d}x  {message}")


def print_ramp_table(summaries: list[dict[str, Any]]) -> None:
    if len(summaries) < 2:
        return
    print("\n─── ramp overview ───")
    print(f"  {'conc':>5}  {'req/s':>8}  {'p50':>8}  {'p95':>8}  {'p99':>8}  {'err%':>6}")
    for summary in summaries:
        lat = summary.get("latency_ms", {})
        print(
            f"  {summary['concurrency']:>5}  {summary['throughput_rps']:>8.1f}"
            f"  {lat.get('p50', float('nan')):>8.1f}  {lat.get('p95', float('nan')):>8.1f}"
            f"  {lat.get('p99', float('nan')):>8.1f}  {summary['error_rate_pct']:>6.2f}"
        )
    print("\n  Knee = the point where req/s stops rising while p95 keeps climbing.")


PROBE_CASES: list[tuple[str, dict[str, str], str]] = [
    ("baseline exact_word", {"q": "aatmaa", "matchMode": "exact_word"}, "200"),
    ("empty q", {"q": ""}, "200 with empty results"),
    ("1-char contains", {"q": "a", "matchMode": "contains"}, "200 empty (dropped pre-guard)"),
    ("2-char contains", {"q": "आत", "matchMode": "contains"}, "400 (guarded)"),
    ("3-char contains", {"q": "आत्म", "matchMode": "contains"}, "200"),
    ("limit over cap", {"q": "dharma", "limit": "99999"}, "200, per_page clamped to 100"),
    ("negative limit", {"q": "dharma", "limit": "-5"}, "200, per_page falls back to 20"),
    ("nan limit", {"q": "dharma", "limit": "abc"}, "200, per_page falls back to 20"),
    ("page far past end", {"q": "dharma", "page": "999999"}, "200 with empty results"),
    ("bogus matchMode", {"q": "dharma", "matchMode": "wat"}, "200, falls back to exact_word"),
    ("ends_with", {"q": "आत्मा", "matchMode": "ends_with"}, "200"),
    ("long q", {"q": "aa" * 400}, "200 or clean 4xx, not a 500"),
    ("fts metachars", {"q": '"aatmaa" OR NEAR/5 *', "matchMode": "exact_word"}, "200, no FTS syntax error"),
    ("sql-ish q", {"q": "aatmaa' OR 1=1 --"}, "200, no 500"),
]


def run_probe(args: argparse.Namespace) -> int:
    print("Param-handling probe (sequential, one request each)\n")
    failures = 0
    for name, overrides, expectation in PROBE_CASES:
        params: dict[str, str] = {
            "q": "aatmaa",
            "limit": str(args.limit),
            "page": str(args.page),
            "matchMode": args.match_mode,
        }
        params.update(overrides)
        url = f"{args.base_url.rstrip('/')}{args.path}?{urllib.parse.urlencode(params, encoding='utf-8')}"
        sample = do_request(url, args, "probe", params["q"][:24])

        detail = ""
        if sample.total_results >= 0:
            detail = f"total={sample.total_results} returned={sample.returned_results}"
            if sample.per_page >= 0:
                detail = f"{detail} per_page={sample.per_page}"
        if sample.error:
            detail = f"{detail} {sample.error}".strip()

        flag = "  "
        if sample.status >= 500 or sample.status == 0:
            flag = "!!"
            failures += 1
        print(
            f"{flag} {name:<22} {sample.status:>3}  {sample.total_ms:7.0f}ms  "
            f"{sample.bytes_read:>7}B  expect {expectation}"
        )
        if detail:
            print(f"     {detail}")
    print(f"\n{len(PROBE_CASES)} cases, {failures} returned 5xx/no-response.")
    return 1 if failures else 0


def load_corpus(path: str | None) -> list[tuple[str, str, str]]:
    if not path:
        return list(DEFAULT_CORPUS)
    entries: list[tuple[str, str, str]] = []
    with open(path, encoding="utf-8") as handle:
        for raw in handle:
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            parts = [p.strip() for p in line.split("\t")] if "\t" in line else [p.strip() for p in line.split(",")]
            roman = parts[0]
            devanagari = parts[1] if len(parts) > 1 else roman
            gujarati = parts[2] if len(parts) > 2 else devanagari
            entries.append((roman, devanagari, gujarati))
    if not entries:
        raise SystemExit(f"corpus file {path} had no usable lines")
    return entries


def write_csv(path: str, samples: list[Sample]) -> None:
    with open(path, "w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            [
                "stage",
                "started_at",
                "query",
                "status",
                "ok",
                "ttfb_ms",
                "total_ms",
                "bytes",
                "api_cache",
                "cdn_cache",
                "total_results",
                "returned_results",
                "error",
            ]
        )
        for s in samples:
            writer.writerow(
                [
                    s.stage,
                    f"{s.started_at:.6f}",
                    s.query,
                    s.status,
                    int(s.ok),
                    "" if math.isnan(s.ttfb_ms) else f"{s.ttfb_ms:.2f}",
                    f"{s.total_ms:.2f}",
                    s.bytes_read,
                    s.api_cache,
                    s.cdn_cache,
                    s.total_results,
                    s.returned_results,
                    s.error,
                ]
            )


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Stress test the library search API.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL, help=f"default {DEFAULT_BASE_URL}")
    parser.add_argument("--path", default=DEFAULT_PATH, help=f"default {DEFAULT_PATH}")
    parser.add_argument("-c", "--concurrency", type=int, default=8, help="in-flight requests (default 8)")
    parser.add_argument("-d", "--duration", type=float, default=None, help="seconds per stage")
    parser.add_argument("-n", "--requests", type=int, default=None, help="total requests per stage")
    parser.add_argument(
        "--ramp",
        default=None,
        help="comma-separated concurrency stages, e.g. 2,5,10,20 (overrides --concurrency)",
    )
    parser.add_argument("--stage-duration", type=float, default=15.0, help="seconds per ramp stage (default 15)")
    parser.add_argument(
        "--cache",
        choices=("bust", "corpus", "warm"),
        default="bust",
        help="bust = unique param per request (cold backend, default); "
        "corpus = rotate queries; warm = replay one URL",
    )
    parser.add_argument("--corpus", default=None, help="file of 'roman,devanagari,gujarati' lines")
    parser.add_argument("--shuffle", action="store_true", help="random corpus order instead of round-robin")
    parser.add_argument("--match-mode", choices=MATCH_MODES, default="exact_word")
    parser.add_argument("--limit", type=int, default=20)
    parser.add_argument("--page", type=int, default=1)
    parser.add_argument("--granths", default=None, help="comma-separated granth ids to filter by")
    parser.add_argument("--no-variants", action="store_true", help="send only q, no queryVariant params")
    parser.add_argument("--timeout", type=float, default=30.0, help="per-request timeout seconds (default 30)")
    parser.add_argument(
        "--user-agent",
        default="ndms-library-stress/1.0 (+scripts/stress_search.py)",
        help="User-Agent header, so this traffic is identifiable in logs",
    )
    parser.add_argument("--think", type=float, default=0.0, help="per-worker sleep between requests")
    parser.add_argument("--warmup", type=int, default=0, help="discarded requests before measuring")
    parser.add_argument("--gzip", action="store_true", help="accept gzip (default identity, honest byte counts)")
    parser.add_argument("--no-parse", action="store_true", help="skip JSON parsing of bodies")
    parser.add_argument("--csv", default=None, help="write raw per-request samples here")
    parser.add_argument("--json", dest="json_out", default=None, help="write the summary JSON here")
    parser.add_argument("--seed", type=int, default=None, help="seed for --shuffle")
    parser.add_argument("--quiet", action="store_true", help="no live progress line")
    parser.add_argument("--probe", action="store_true", help="run the param-handling probe instead of load")
    parser.add_argument("--yes", action="store_true", help="skip the confirmation prompt for heavy runs")

    args = parser.parse_args(argv)

    if args.probe:
        return args
    if args.duration is None and args.requests is None:
        args.duration = 15.0
    if args.concurrency < 1:
        parser.error("--concurrency must be >= 1")
    return args


def confirm_heavy(args: argparse.Namespace, stages: list[int]) -> bool:
    peak = max(stages)
    remote = "localhost" not in args.base_url and "127.0.0.1" not in args.base_url
    if args.yes or not remote or peak <= 20:
        return True
    print(
        f"About to drive up to {peak} concurrent requests at {args.base_url}.\n"
        f"With --cache {args.cache} that hits the real backend (Turso + Supabase) on every request.\n"
        "Only run this against infrastructure you own. Continue? [y/N] ",
        end="",
    )
    try:
        return input().strip().lower() in {"y", "yes"}
    except EOFError:
        return False


def main(argv: list[str]) -> int:
    args = parse_args(argv)

    if args.probe:
        return run_probe(args)

    if args.seed is not None:
        random.seed(args.seed)

    corpus = load_corpus(args.corpus)
    stages = [int(x) for x in args.ramp.split(",") if x.strip()] if args.ramp else [args.concurrency]
    if any(c < 1 for c in stages):
        raise SystemExit("--ramp values must be >= 1")

    if not confirm_heavy(args, stages):
        print("aborted")
        return 130

    target = f"{args.base_url.rstrip('/')}{args.path}"
    print(f"target      {target}")
    print(f"cache mode  {args.cache}   match mode {args.match_mode}   limit {args.limit}")
    print(f"corpus      {len(corpus)} queries{' (shuffled)' if args.shuffle else ''}")
    print(f"stages      {stages}   " + (f"{args.duration}s each" if args.duration else f"{args.requests} req each"))
    print(f"sample url  {build_url(args, corpus[0], '0' if args.cache == 'bust' else None)}")

    if args.warmup > 0:
        print(f"\nwarming up ({args.warmup} discarded requests)...")
        run_stage(args, corpus, "warmup", min(args.warmup, stages[0]), None, args.warmup)

    all_samples: list[Sample] = []
    summaries: list[dict[str, Any]] = []
    interrupted = False

    try:
        for concurrency in stages:
            duration = args.duration if not args.ramp else (args.duration or args.stage_duration)
            stage = run_stage(
                args,
                corpus,
                f"c{concurrency}",
                concurrency,
                duration if args.requests is None else None,
                args.requests,
            )
            all_samples.extend(stage.samples)
            summary = summarize(stage)
            summaries.append(summary)
            print_summary(summary)
    except KeyboardInterrupt:
        interrupted = True
        print("\n\ninterrupted — reporting what completed so far")

    print_ramp_table(summaries)

    if args.csv:
        write_csv(args.csv, all_samples)
        print(f"\nwrote {len(all_samples)} samples to {args.csv}")
    if args.json_out:
        with open(args.json_out, "w", encoding="utf-8") as handle:
            json.dump(
                {"target": target, "cache_mode": args.cache, "stages": summaries},
                handle,
                indent=2,
                ensure_ascii=False,
            )
        print(f"wrote summary to {args.json_out}")

    if interrupted:
        return 130
    worst = max((s["error_rate_pct"] for s in summaries), default=0.0)
    return 1 if worst > 0 else 0


if __name__ == "__main__":
    try:
        sys.exit(main(sys.argv[1:]))
    except KeyboardInterrupt:
        sys.exit(130)

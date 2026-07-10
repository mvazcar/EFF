"""
Download the EFF microdata archives from Banco de España into raw_zipped/.

This is step 0, and it is OPTIONAL. The pipeline's contract starts at raw_zipped/, holding the
archives exactly as Banco de España names them, flat, the same as MCVL's raw_zipped/. If you
already downloaded them through the browser, drop them there and skip straight to unpack.py.

Unlike ECV (INE publishes openly), EFF microdata is released for scientific use behind a
registration wall, so this script cannot be fully self-service:

    1. Register once per wave at
           https://app.bde.es/gnt_seg/controlAccesoEmail.jsp?pas=eff&lang=es&p1=2022
       accepting the research-use-only conditions. BdE mails you a link of the form
           https://pas.bde.es/iae/veriurl/?mail=...&ticket=...&url=/privbde/es/pas/eff-datos/...

    2. Export that link and run this script. It opens the session, discovers every wave page,
       and downloads what you ask for.

           export EFF_ACCESS_URL='https://pas.bde.es/iae/veriurl/?mail=...&ticket=...'
           python download.py

The link embeds your email and an auth ticket. Treat it as a credential: it is read from the
environment precisely so that it never lands in the repo, a shell history file, or a log.

Waves 2017 and 2020 are served from the public www.bde.es file server, so they download with
no session at all — running `python download.py --waves 2017 2020` needs no EFF_ACCESS_URL.

    python download.py                          # default groups, all waves
    python download.py --waves 2020 2022
    python download.py --groups imputed derived # skip the 700 MB of replicate weights
    python download.py --groups replicate       # ... and now fetch them
    python download.py --formats dta csv        # both builds
    python download.py --list                   # show what would be fetched, download nothing

Downloads run concurrently and are idempotent: an archive whose local size matches the
server's Content-Length is skipped.
"""
from __future__ import annotations

import argparse
import html
import http.cookiejar
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from config import (
    ACCESS_URL, DEFAULT_GROUPS, FORMAT_FILTERED, FORMATS, GROUP_ORDER, GROUPS, LANDING,
    PUBLIC_HOST, PUBLIC_WAVES, PRIVATE_HOST, RAW_ZIPPED, WAVES,
)

UA = "Mozilla/5.0 (compatible; eff-pipeline/1.0; +https://github.com/mvazcar)"
TIMEOUT = 900

# Public-wave assets, constructed rather than scraped so that 2017/2020 work without a session.
PUBLIC_DIR = f"{PUBLIC_HOST}/f/webbde/SES/estadis/eff/ficheros"

_ANCHOR = re.compile(r"""(?is)<a\b[^>]*href=["']([^"']+)["']""")
_ASSET = re.compile(r"\.(zip|pdf|docx?)$", re.I)


def _opener() -> urllib.request.OpenerDirector:
    jar = http.cookiejar.CookieJar()
    op = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(jar))
    op.addheaders = [("User-Agent", UA)]
    return op


def _get(op, url: str) -> bytes:
    with op.open(urllib.request.Request(url), timeout=TIMEOUT) as r:
        return r.read()


def _remote_size(op, url: str) -> int | None:
    try:
        req = urllib.request.Request(url, method="HEAD")
        with op.open(req, timeout=120) as r:
            n = r.headers.get("Content-Length")
            return int(n) if n else None
    except Exception:                                                # noqa: BLE001
        return None


def classify(name: str) -> str | None:
    """Which download group an asset belongs to. Order matters: `databol_2008_dta_census2001.zip`
    also matches the plain `derived` pattern, and the label .do programs ride inside a csv
    archive that also matches `imputed`. config.GROUP_ORDER encodes the precedence."""
    for g in GROUP_ORDER:
        if re.search(GROUPS[g], name, re.I):
            return g
    return None


# ── Discovery ──────────────────────────────────────────────────────────────
def _first_existing(op, urls: list[str]) -> str | None:
    """The first URL that answers a HEAD with a size. Used where BdE filed the same document
    under different language directories in different waves."""
    for u in urls:
        if _remote_size(op, u) is not None:
            return u
    return None


def _public_assets(op, wave: int) -> list[tuple[str, str]]:
    """
    (name, url) for a public wave, constructed rather than scraped so that 2017 and 2020 work
    with no session at all.

    The data archives follow one pattern. The two documents do not: `definitions_2017.docx` is
    filed under `en/` and `definitions_2020.docx` under `es/`, and the results PDF is named
    `EFF<wave>_DatosActualizados.pdf`. Rather than encode that as a table of exceptions that will
    rot, probe both candidates with a HEAD and keep whichever answers.
    """
    out: list[tuple[str, str]] = []
    for i in (1, 2, 3, 4, 5):
        for ext in ("dta", "csv"):
            out.append((f"eff_{wave}_imp{i}_{ext}.zip", f"{PUBLIC_DIR}/es/eff_{wave}_imp{i}_{ext}.zip"))
    for stem in ("sombra", "replicate_weights", "databol"):
        for ext in ("dta", "csv"):
            n = f"{stem}_{wave}_{ext}.zip"
            out.append((n, f"{PUBLIC_DIR}/es/{n}"))

    for name in (f"definitions_{wave}.docx", f"EFF{wave}_DatosActualizados.pdf"):
        url = _first_existing(op, [f"{PUBLIC_DIR}/{lang}/{name}" for lang in ("es", "en")])
        if url:
            out.append((name, url))
    return out


def _scrape(op, page_url: str) -> list[tuple[str, str]]:
    body = _get(op, page_url).decode("latin-1", "replace")
    out, seen = [], set()
    for href in _ANCHOR.findall(body):
        href = html.unescape(href)
        if not _ASSET.search(href.split("?")[0]):
            continue
        if "/eff-datos/" not in href and "/estadis/eff/" not in href:
            continue
        url = urllib.parse.urljoin(PUBLIC_HOST if href.startswith("/f/") else PRIVATE_HOST, href)
        name = href.rsplit("/", 1)[-1]
        if name in seen:
            continue
        seen.add(name)
        out.append((name, url))
    return out


def _page_wave(assets: list[tuple[str, str]]) -> int | None:
    """
    Which wave a scraped page belongs to.

    The gated area files each wave's assets under `.../eff-datos/<year>/`, so read the year off
    the URL path. Only fall back to counting years in the filenames if no path says so — and note
    why that fallback is second choice: `databol_2002_dta_census2001.zip` contains both 2002 and
    2001, so a naive count over the 2002 page sees six spurious "2001"s.
    """
    for _, url in assets:
        m = re.search(r"/eff-datos/(\d{4})/", url)
        if m:
            return int(m.group(1))
    years = [int(y) for n, _ in assets for y in re.findall(r"(20\d\d)", n)]
    return max(set(years), key=years.count) if years else None


def discover(op, waves: list[int], authed: bool) -> dict[int, list[tuple[str, str]]]:
    """Map wave -> [(filename, url)]. Scrapes the private area when authenticated."""
    found: dict[int, list[tuple[str, str]]] = {}
    if authed:
        landing = _get(op, LANDING).decode("latin-1", "replace")
        pages = [urllib.parse.urljoin(PRIVATE_HOST, html.unescape(h))
                 for h in _ANCHOR.findall(landing) if "/eff-datos/" in h and h.endswith(".html")]
        for page in dict.fromkeys(pages):
            assets = _scrape(op, page)
            if not assets:
                continue
            wave = _page_wave(assets)
            if wave in waves:
                found.setdefault(wave, []).extend(assets)
    for w in waves:
        if w not in found and w in PUBLIC_WAVES:
            found[w] = _public_assets(op, w)
    return found


# ── Fetch ──────────────────────────────────────────────────────────────────
def fetch(op, wave: int, name: str, url: str, force: bool) -> tuple[str, str, int]:
    # Flat, and named exactly as BdE serves it — the same contract as MCVL's raw_zipped/. A user
    # who downloaded by hand through the browser gets a byte-identical directory, and unpack.py
    # cannot tell the two apart. Nothing downstream depends on download.py having run.
    dest = RAW_ZIPPED / name
    dest.parent.mkdir(parents=True, exist_ok=True)

    if dest.exists() and not force:
        want = _remote_size(op, url)
        have = dest.stat().st_size
        if want is None or want == have:
            return name, "skip", have

    tmp = dest.with_suffix(dest.suffix + ".part")
    try:
        with op.open(urllib.request.Request(url), timeout=TIMEOUT) as r, open(tmp, "wb") as f:
            while chunk := r.read(1 << 20):
                f.write(chunk)
    except urllib.error.HTTPError as e:
        tmp.unlink(missing_ok=True)
        return name, f"HTTP {e.code}", 0
    except Exception as e:                                           # noqa: BLE001
        tmp.unlink(missing_ok=True)
        return name, f"ERR {type(e).__name__}", 0

    # BdE serves an HTML error page with 200 for an expired session; a real archive starts
    # with the zip local-file-header magic. Catching this here beats a confusing unpack crash.
    if name.lower().endswith(".zip"):
        with open(tmp, "rb") as f:
            if f.read(4) != b"PK\x03\x04":
                tmp.unlink(missing_ok=True)
                return name, "NOT A ZIP", 0

    tmp.replace(dest)
    return name, "ok", dest.stat().st_size


def main() -> int:
    sys.stdout.reconfigure(encoding="utf-8")
    ap = argparse.ArgumentParser(description="Download EFF microdata archives from Banco de España")
    ap.add_argument("--waves", nargs="+", type=int, default=None, help=f"subset of {WAVES}")
    ap.add_argument("--groups", nargs="+", default=list(DEFAULT_GROUPS),
                    help=f"any of {sorted(GROUPS)} (default: {' '.join(DEFAULT_GROUPS)})")
    ap.add_argument("--formats", nargs="+", default=["dta"], choices=list(FORMATS))
    ap.add_argument("--workers", type=int, default=4, help="concurrent downloads (default 4)")
    ap.add_argument("--force", action="store_true", help="re-download even if present")
    ap.add_argument("--list", action="store_true", help="print the plan and exit")
    args = ap.parse_args()

    waves = [w for w in WAVES if not args.waves or w in set(args.waves)]
    groups = set(args.groups)
    if bad := groups - set(GROUPS):
        print(f"unknown group(s): {sorted(bad)}; choose from {sorted(GROUPS)}", file=sys.stderr)
        return 2

    op = _opener()
    authed = False
    if ACCESS_URL:
        try:
            _get(op, ACCESS_URL)                       # sets the session cookies
            authed = True
        except Exception as e:                                       # noqa: BLE001
            print(f"could not open the EFF session: {e}", file=sys.stderr)

    gated = [w for w in waves if w not in PUBLIC_WAVES]
    if gated and not authed:
        print(f"waves {gated} need a session but EFF_ACCESS_URL is unset or expired.\n"
              f"Register at {'https://app.bde.es/gnt_seg/controlAccesoEmail.jsp?pas=eff&lang=es&p1=2022'}\n"
              f"and export the link BdE mails you as EFF_ACCESS_URL.\n"
              f"Waves 2017 and 2020 are public and need no session.", file=sys.stderr)

    assets = discover(op, waves, authed)
    if not assets:
        print("nothing discovered", file=sys.stderr)
        return 1

    # `eff_<wave>_imp1_csv.zip` is two things at once: the first implicate of the csv build, and
    # the only carrier of the label .do programs. classify() must pick one group, and it picks
    # `labels`. So a csv user asking for `--groups imputed --formats csv` would otherwise receive
    # implicates 2..5 and silently lose implicate 1. Claim it for `imputed` too, in that case.
    wants_csv_implicate1 = "imputed" in groups and "csv" in args.formats

    plan: list[tuple[int, str, str]] = []
    for wave in sorted(assets):
        for name, url in assets[wave]:
            g = classify(name)
            if g == "labels" and wants_csv_implicate1:
                plan.append((wave, name, url))
                continue
            if g is None or g not in groups:
                continue
            if g in FORMAT_FILTERED and not any(name.endswith(f"_{ext}.zip") for ext in args.formats):
                continue
            plan.append((wave, name, url))
    plan = list(dict.fromkeys(plan))

    print(f"EFF download: {len(plan)} assets from {len(assets)} waves "
          f"(groups: {' '.join(sorted(groups))}; formats: {' '.join(args.formats)})"
          f"{'  [authenticated]' if authed else '  [public only]'}\n")
    if args.list:
        for wave, name, url in plan:
            print(f"  {wave}  {name:<44} {url}")
        return 0
    if not plan:
        print("nothing selected", file=sys.stderr)
        return 1

    t0 = time.time()
    total = ok = skipped = 0
    failures: list[tuple[int, str, str]] = []
    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futs = {ex.submit(fetch, op, w, n, u, args.force): (w, n) for w, n, u in plan}
        for fut in as_completed(futs):
            wave, _ = futs[fut]
            name, status, size = fut.result()
            total += size
            if status == "ok":
                ok += 1
            elif status == "skip":
                skipped += 1
            else:
                failures.append((wave, name, status))
            print(f"  {status:10s} {wave}  {name:<44} {size/1e6:8.1f} MB")

    print(f"\n{ok} downloaded, {skipped} already present, {len(failures)} failed"
          f"  ({total/1e6:.0f} MB, {time.time()-t0:.0f}s)  -> {RAW_ZIPPED}")
    for w, n, s in failures:
        print(f"  FAILED {w} {n}: {s}", file=sys.stderr)
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())

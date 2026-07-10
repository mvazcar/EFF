"""
Unpack and route the EFF raw archives from raw_zipped/ to raw/, catalog/labels/ and docs/.

Full flow:
    raw_zipped/eff_{YEAR}_imp{i}_dta.zip  -->  unzip  -->  route by wave and kind  -->
        raw/{YEAR}/otras_secciones_{YEAR}_imp{i}.dta
        raw/{YEAR}/seccion6_{YEAR}_imp{i}.dta

raw_zipped/ is flat and holds the archives EXACTLY as Banco de España serves them, the same
contract as MCVL's raw_zipped/. Drop them there by hand from the browser, or let download.py
fetch them; this script does not care which.

Every archive is a plain single-level zip — no zip-in-zip, no 7z, no RAR — so this is far simpler
than the ECV or Panel de Hogares equivalents. What it does have to get right is ROUTING, because
the archives disagree about where their content belongs:

  Wave inferred from    eff_2022_imp1_dta.zip, sombra_2011_dta.zip, databol_2017_dta.zip
  the filename          -> raw/2022/, raw/2011/, raw/2017/

  Year-colliding name   databol_2008_dta_census2001.zip carries both 2008 and 2001.
                        2001 is not a wave, so filtering candidates against ALL_WAVES resolves it.

  No year at all        imp1_version1.8_dta.zip, W_version1.8_csv.zip, labels.zip,
                        UDB1-HFCSdescription.pdf, Notes_comparability_EFF_HFCS.doc
                        These are the ECB HFCS re-release, published on the 2008 page only.
                        config.HFCS_ONLY_WAVE routes them to raw/2008/.

  Colliding contents    Every wave ships databol1.dta .. databol5.dta under those exact names,
                        with no year. A flat raw/ would have eight waves overwriting each other,
                        which is why raw/ is per-wave even though raw_zipped/ is not.
                        The _census2001 variants keep their suffix and so never collide.

  Not data              etiquetas_*.do (Stata label programs, 2011+) -> catalog/labels/{YEAR}/
                        definitions_*.doc[x], *cuadros*.pdf         -> docs/

Usage:
    python unpack.py                     # dry run -- shows what would happen
    python unpack.py --execute           # actually extract
    python unpack.py --execute --waves 2020 2022
    python unpack.py --execute --force   # overwrite existing extracts

Idempotent: already-extracted, non-empty files are kept, so re-running is cheap.
"""
from __future__ import annotations

import argparse
import re
import sys
import time
import zipfile
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from config import CATALOG, DOCS, RAW_DIR, RAW_ZIPPED, ROOT, WAVES, raw_dir, wave_of

DATA_RE = re.compile(r"\.(dta|csv)$", re.I)
LABEL_RE = re.compile(r"\.do$", re.I)
DOC_RE = re.compile(r"\.(pdf|docx?)$", re.I)


def plan_wave(wave: int, archives: list[Path], force: bool) -> list[tuple[Path, Path]]:
    """(source archive, destination path) for everything one wave's archives would produce."""
    dest, lab_dir = raw_dir(wave), CATALOG / "labels" / str(wave)
    out: list[tuple[Path, Path]] = []

    for archive in sorted(archives):
        if DOC_RE.search(archive.name):                       # a loose .pdf / .doc, not an archive
            out.append((archive, DOCS / archive.name))
            continue
        if archive.suffix.lower() != ".zip":
            continue
        try:
            zf = zipfile.ZipFile(archive)
        except zipfile.BadZipFile:
            print(f"    !! {archive.name}: not a zip (re-download it)", file=sys.stderr)
            continue
        with zf:
            for m in zf.infolist():
                if m.is_dir():
                    continue
                base = Path(m.filename).name
                if DATA_RE.search(base):
                    out.append((archive, dest / base))
                elif LABEL_RE.search(base):
                    out.append((archive, lab_dir / base))
                elif DOC_RE.search(base):
                    out.append((archive, DOCS / base))
    if not force:
        out = [(a, t) for a, t in out if not (t.exists() and t.stat().st_size > 0)]
    return out


def unpack_wave(wave: int, archives: list[Path], force: bool) -> tuple[int, int, int, int]:
    """Extract one wave's archives. Returns (wave, n_data, n_labels, n_docs)."""
    dest, lab_dir = raw_dir(wave), CATALOG / "labels" / str(wave)
    n_data = n_lab = n_doc = 0

    for archive in sorted(archives):
        if DOC_RE.search(archive.name):
            target = DOCS / archive.name
            if force or not (target.exists() and target.stat().st_size > 0):
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_bytes(archive.read_bytes())
            n_doc += 1
            continue
        if archive.suffix.lower() != ".zip":
            continue
        try:
            zf = zipfile.ZipFile(archive)
        except zipfile.BadZipFile:
            print(f"    !! {archive.name}: not a zip (re-download it)", file=sys.stderr)
            continue

        with zf:
            for m in zf.infolist():
                if m.is_dir():
                    continue
                base = Path(m.filename).name
                if DATA_RE.search(base):
                    target, kind = dest / base, "data"
                elif LABEL_RE.search(base):
                    target, kind = lab_dir / base, "lab"
                elif DOC_RE.search(base):
                    target, kind = DOCS / base, "doc"
                else:
                    continue
                if not (target.exists() and target.stat().st_size > 0 and not force):
                    target.parent.mkdir(parents=True, exist_ok=True)
                    with zf.open(m) as fh, open(target, "wb") as out:
                        while chunk := fh.read(1 << 20):
                            out.write(chunk)
                n_data += kind == "data"
                n_lab += kind == "lab"
                n_doc += kind == "doc"

    return wave, n_data, n_lab, n_doc


def route(waves: list[int]) -> tuple[dict[int, list[Path]], list[Path]]:
    """Group raw_zipped/ by wave. Second element: files whose wave could not be established."""
    by_wave: dict[int, list[Path]] = defaultdict(list)
    unresolved: list[Path] = []
    for p in sorted(RAW_ZIPPED.iterdir()):
        if not p.is_file():
            continue
        w = wave_of(p.name)
        if w is None:
            unresolved.append(p)
        elif w in waves:
            by_wave[w].append(p)
    return by_wave, unresolved


def main() -> int:
    sys.stdout.reconfigure(encoding="utf-8")
    ap = argparse.ArgumentParser(description="Unpack EFF archives: raw_zipped/ -> raw/<wave>/")
    ap.add_argument("--execute", action="store_true", help="actually extract (default: dry run)")
    ap.add_argument("--waves", nargs="+", type=int, default=None)
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--force", action="store_true", help="overwrite existing extracts")
    args = ap.parse_args()

    if not RAW_ZIPPED.exists() or not any(RAW_ZIPPED.iterdir()):
        print(f"{RAW_ZIPPED} is empty.\n"
              f"  Place the EFF archives there, or run: python download.py", file=sys.stderr)
        return 1

    waves = [w for w in WAVES if not args.waves or w in set(args.waves)]
    by_wave, unresolved = route(waves)

    if unresolved:
        print(f"{len(unresolved)} file(s) whose wave could not be established — SKIPPED:")
        for p in unresolved:
            print(f"    {p.name}")
        print()

    if not by_wave:
        print("nothing to unpack for the selected waves", file=sys.stderr)
        return 1

    if not args.execute:
        print(f"DRY RUN — nothing will be written. Re-run with --execute.\n")
        print(f"  raw_zipped/  ({sum(len(v) for v in by_wave.values())} archives, flat)")
        print(f"      |")
        print(f"      +-- unzip + route -->  raw/<wave>/ , catalog/labels/<wave>/ , docs/\n")
        total = 0
        for wave in sorted(by_wave):
            todo = plan_wave(wave, by_wave[wave], args.force)
            total += len(todo)
            roots: dict[str, int] = defaultdict(int)
            for _, t in todo:
                try:
                    rel = t.parent.relative_to(ROOT)
                except ValueError:              # EFF_RAW_DIR may point outside the project
                    rel = t.parent
                roots[rel.as_posix()] += 1
            summary = ", ".join(f"{n} -> {p}/" for p, n in sorted(roots.items()))
            print(f"  {wave}  {len(by_wave[wave]):2d} archives  "
                  f"{len(todo):3d} file(s) to write   {summary or '(all present)'}")
        print(f"\n  {total} file(s) would be written. Nothing done.")
        return 0

    print(f"EFF unpack: {len(by_wave)} waves, "
          f"{sum(len(v) for v in by_wave.values())} archives\n")
    t0 = time.time()
    tot_d = tot_l = tot_x = 0
    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futs = {ex.submit(unpack_wave, w, a, args.force): w for w, a in by_wave.items()}
        for fut in as_completed(futs):
            wave, nd, nl, nx = fut.result()
            tot_d, tot_l, tot_x = tot_d + nd, tot_l + nl, tot_x + nx
            print(f"  {wave}   {nd:3d} data  {nl:2d} label .do  {nx:2d} docs")

    print(f"\n{tot_d} data files, {tot_l} label programs, {tot_x} docs in {time.time()-t0:.0f}s")
    print(f"  -> {RAW_DIR}\n  -> {CATALOG / 'labels'}\n  -> {DOCS}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

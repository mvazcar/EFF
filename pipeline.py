"""
Build orchestrator for the EFF pipeline.

Pipeline flow
-------------
    raw_zipped/*.zip                     archives as received (flat)   [download.py, optional]
        |
        +-- unpack.py --execute
        |       +--> raw/<wave>/*.dta               data, one directory per wave
        |       +--> catalog/labels/<wave>/*.do     Stata label programs (2011+)
        |       +--> docs/*.pdf,*.doc[x]            BdE documentation
        |
        v
    Step 01  household   raw/<wave>/otras_secciones_*  -> temp/household_<wave>.parquet
    Step 02  members     raw/<wave>/seccion6_*         -> temp/members_<wave>.parquet
    Step 03  derived     raw/<wave>/databol*           -> temp/derived_<wave>.parquet
    Step 04  weights     raw/<wave>/replicate_*        -> temp/weights_<wave>_<kind>.parquet
    Step 05  panel       temp/household_<wave>         -> output/panel_bridge.parquet
    Step 06  pool        temp/derived_<wave> + bridge  -> output/eff_derived_panel.parquet

Steps 01-04 read raw/ only, so they are independent of each other and run as parallel
subprocesses (large allocations are released when each process exits). Step 05 needs every
wave's household parquet; step 06 needs every wave's derived parquet and the bridge.

    01 household ─┬────────────► 05 panel ──┐
    02 members    │                         ├─► 06 pool
    03 derived  ──┴─────────────────────────┘
    04 weights   (independent; only needed for standard errors)

The STEPS table below is the single source of truth for that graph. `run.py --list` prints it,
the dependency order is derived from it, and nothing else in the repo hardcodes a step id.
"""
from __future__ import annotations

import subprocess
import sys
import time
from dataclasses import dataclass, field
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))
from config import OUTPUT


@dataclass(frozen=True)
class Step:
    id: str
    module: str
    title: str
    inputs: tuple[str, ...]
    outputs: tuple[str, ...]
    depends_on: tuple[str, ...] = field(default=())
    optional: bool = False          # may legitimately fail when its input was not downloaded


STEPS: tuple[Step, ...] = (
    Step("01", "step01_household", "household table",
         inputs=("raw/<wave>/otras_secciones_<wave>_imp{1..5}.dta",),
         outputs=("temp/household_<wave>.parquet",)),
    Step("02", "step02_members", "member table",
         inputs=("raw/<wave>/seccion6_<wave>_imp{1..5}.dta",
                 "raw/<wave>/otras_secciones_<wave>_imp1.dta  (for p1)"),
         outputs=("temp/members_<wave>.parquet",)),
    Step("03", "step03_derived", "derived variables",
         inputs=("raw/<wave>/databol{1..5}.dta",),
         outputs=("temp/derived_<wave>.parquet", "temp/derived_full_<wave>.parquet")),
    Step("04", "step04_weights", "replicate weights",
         inputs=("raw/<wave>/replicate_weights_<wave>.dta",
                 "raw/<wave>/replicate_pan{1,2}weights_<wave>.dta"),
         outputs=("temp/weights_<wave>_<kind>.parquet",),
         optional=True),
    Step("05", "step05_panel", "cross-wave household bridge",
         inputs=("temp/household_<wave>.parquet  (all waves)",),
         outputs=("output/panel_bridge.parquet",),
         depends_on=("01",)),
    Step("06", "step06_pool", "pooled derived panel",
         inputs=("temp/derived_<wave>.parquet  (all waves)", "output/panel_bridge.parquet"),
         outputs=("output/eff_derived_panel.parquet",),
         depends_on=("03", "05")),
)

BY_ID = {s.id: s for s in STEPS}


def describe() -> str:
    """The data-flow contract, rendered. `run.py --list` prints this."""
    lines = ["EFF pipeline — input -> output", ""]
    for s in STEPS:
        dep = f"  (after {', '.join(s.depends_on)})" if s.depends_on else ""
        opt = "  [optional]" if s.optional else ""
        lines.append(f"  Step {s.id}  {s.title}{dep}{opt}")
        for i in s.inputs:
            lines.append(f"      IN   {i}")
        for o in s.outputs:
            lines.append(f"      OUT  {o}")
        lines.append("")
    return "\n".join(lines)


def _run(step: Step) -> tuple[Step, int, float, str]:
    t0 = time.time()
    r = subprocess.run([sys.executable, str(ROOT / f"{step.module}.py")],
                       capture_output=True, text=True, encoding="utf-8", errors="replace")
    return step, r.returncode, time.time() - t0, (r.stdout or "") + (r.stderr or "")


def _selection(only: list[str] | None, resume_from: str | None) -> list[Step]:
    if only:
        want = set(only)
        return [s for s in STEPS if s.id in want]
    if resume_from:
        return [s for s in STEPS if s.id >= resume_from]
    return list(STEPS)


def run_pipeline(only: list[str] | None = None, workers: int = 3, parallel: bool = True,
                 resume_from: str | None = None) -> list[str]:
    """
    Run the selected steps in dependency order.

    Returns the ids of the steps that failed, so run.py can exit non-zero — a build that printed
    a traceback must not look like success to a Makefile.
    """
    sel = _selection(only, resume_from)
    indep = [s for s in sel if not s.depends_on]
    dependent = [s for s in sel if s.depends_on]

    print("EFF — BUILD pipeline (mvazcar/MCVL-style)")
    print(f"  independent : {[s.id for s in indep]}"
          f"  ({'parallel x' + str(workers) if parallel else 'serial'})")
    print(f"  then, in order: {[s.id for s in dependent]}\n")
    t0 = time.time()
    failed: list[str] = []

    if indep:
        if parallel and len(indep) > 1:
            with ThreadPoolExecutor(max_workers=workers) as ex:
                futs = [ex.submit(_run, s) for s in indep]
                for fut in as_completed(futs):
                    step, rc, dt, out = fut.result()
                    last = next((ln for ln in reversed(out.splitlines()) if ln.strip()), "")
                    print(f"[{step.id}] rc={rc}  {dt:>5.1f}s  | {last.strip()[:88]}")
                    if rc != 0:
                        failed.append(step.id)
                        print("\n".join("     " + ln for ln in out.splitlines()[-3:]))
        else:
            for s in indep:
                step, rc, dt, out = _run(s)
                print(f"[{step.id}] rc={rc}  {dt:>5.1f}s")
                if rc != 0:
                    failed.append(step.id)

    for s in dependent:
        missing = [d for d in s.depends_on if d in failed]
        if missing:
            print(f"\n[{s.id}] SKIPPED — depends on failed step(s) {missing}")
            failed.append(s.id)
            continue
        print(f"\n[{s.id}] {s.title} ...")
        step, rc, dt, out = _run(s)
        for ln in out.splitlines():
            if any(k in ln for k in ("->", "waves", "households", "rows", "spell", "split")):
                print("   " + ln.strip())
        print(f"[{s.id}] rc={rc}  {dt:.1f}s")
        if rc != 0:
            failed.append(s.id)

    # Every failure counts. A step whose optional input is simply absent reports success and says
    # so (step04_weights does exactly that), so anything that reaches here is a real failure.
    print(f"\nBUILD {'COMPLETE' if not failed else 'FINISHED WITH FAILURES: ' + ','.join(failed)}"
          f" in {(time.time() - t0) / 60:.1f} min  ->  {OUTPUT}")
    return failed

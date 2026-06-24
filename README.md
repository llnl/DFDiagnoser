# DFDiagnoser

DFDiagnoser turns [DFAnalyzer](https://github.com/LLNL/dfanalyzer)'s per-window
**analysis facts** into **longitudinal diagnosis findings** — *is this bottleneck
persistent, getting worse, where, and what class of fix does it call for* — that
[DFOptimizer](https://github.com/LLNL/dfoptimizer) can act on. It runs the same way
**offline** (replay a saved fact bundle) and **online** (consume a live stream over
Mofka or ZMQ), producing identical findings.

## What it does

DFAnalyzer emits `analyzer.fact-envelope.v1` facts — one per analysis window per
view (a temporal view like `window`/`epoch`/`step`/`time_range`, or a spatial view
like `file_name`/`proc_name`). Each fact carries a `fact_type` (e.g.
`fetch_pressure`), a two-level `scope` (`layer:view` aggregate, or
`layer:view:entity` detail), a continuous `severity`, and `opportunity_tags`.

DFDiagnoser is a **pure fact consumer** (scoring/fact-building live in the analyzer).
It tracks each `(fact_type, scope)` **along its temporal axis** and summarizes the
trajectory:

- **persistence** — longest run of consecutive windows the fact appears in
- **prevalence** — fraction of windows it appears in
- **trend** — improving / stable / worsening
- **motif** — the classified pattern (e.g. `persistent_pressure`, `metadata_bound`)
- **recommendation_bundle** + **opportunity_tags** — the class of fix (e.g.
  `input_pipeline_tuning`)

Temporal views (`window` online; `epoch`/`step`/`time_range` offline) are the
longitudinal axis; spatial views (`file`/`proc`/`host`) are one-shot. Online, it can
also emit per-window **control findings** so the optimizer acts on fresh state.

## Installation

```bash
pip install dfdiagnoser                 # core (offline)
pip install "dfdiagnoser[streaming]"    # + online transports (pyzmq / mofka)
```

From source:

```bash
git clone https://github.com/LLNL/dfdiagnoser.git && cd dfdiagnoser
uv sync && uv pip install -e .
```

## Usage

The CLI is a Hydra app selecting an `input` (`file` / `mofka` / `zmq`) and an
`output` (`console` / `file`).

### Offline — replay a DFAnalyzer fact bundle

```bash
# 1. DFAnalyzer writes a fact bundle (facts.jsonl) with output=file
dfanalyzer analyzer/preset=dlio trace_path=tests/data/extracted/dftracer-dlio \
    view_types=[epoch] facts.enabled=true \
    output=file output.path=/tmp/bundle

# 2. DFDiagnoser replays it into findings
dfdiagnoser input=file input.path=/tmp/bundle output=console
```

```text
╭─ DFDiagnoser Diagnosis ─╮
│ Findings   1            │
│ Scopes     1            │
│ Severity   critical: 1  │
╰─────────────────────────╯
Findings
└── view_type: epoch (1)
    └── app:epoch (1)
        └── [C] fetch_pressure: persistent_pressure (severity critical 1.00, conf 0.80)
            ├── prevalence 1.00, persistence 5, trend stable -> input_pipeline_tuning
            └── (fact) fetch_pressure @ app:epoch
```

(Read it as: `fetch_pressure` in the `app` layer was `critical` in **all 5** epochs —
`persistent_pressure` — so the fix class is `input_pipeline_tuning`. Online over the
`window` axis it reads identically, with persistence counted across streaming windows.)

### Online — live stream (ZMQ)

DFAnalyzer streams fact envelopes with `output=zmq`; DFDiagnoser consumes them live
and prints the longitudinal summary on idle (or, with `input.output_address` set,
streams control findings onward to the optimizer):

```bash
# DFDiagnoser binds and waits for facts
dfdiagnoser input=zmq input.address="tcp://*:5556" output=console

# DFAnalyzer side (separate process) pushes facts to it
dfanalyzer analyzer/preset=dlio input=zmq input.address="tcp://*:5555" \
    view_types=[window] facts.enabled=true \
    output=zmq output.address="tcp://127.0.0.1:5556"
```

### Online — live stream (Mofka, LiveFlow)

```bash
dfdiagnoser input=mofka \
    input.group_file="$MOFKA_GROUP_FILE" \
    input.topic_name=analyzer_facts \
    input.output_topic=diagnosis_findings \
    output=console
```

All three paths yield the same `DiagnosisResult.findings`; only the transport differs.

## Inputs and outputs

- **Input:** `analyzer.fact-envelope.v1` envelopes — a `.jsonl` file / bundle dir
  (offline), or a Mofka/ZMQ stream (online).
- **Output:** findings rendered to the console, written to JSON (`output=file`), or
  streamed onward. Each finding's wire form carries `finding_type`, `scope`, `motif`,
  `severity_score`, `prevalence`, `persistence`, `trend_direction`,
  `opportunity_tags`, `recommendation_bundle`, and `key_metrics` — the fields
  DFOptimizer gates actuation on.

## Requirements

- Python >= 3.9
- DFAnalyzer fact envelopes (run DFAnalyzer with `facts.enabled=true`)
- Online transports: `pyzmq` (ZMQ) or `mochi-mofka` (Mofka), via the `[streaming]` extra

## License

MIT — see [LICENSE](LICENSE).

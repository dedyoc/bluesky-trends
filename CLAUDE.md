# bluesky-trends — pipeline logic

Real-time social analytics on the Bluesky firehose. This repo holds CODE ONLY:
ingest service, Flink trend job, Dagster assets, dbt models, FastAPI trends API.
Kubernetes manifests live in the separate `homelab-ops` repo — never write k8s
YAML here beyond local dev (docker-compose) files.

## Always-true rules
- Never edit generated code (`*_pb2.py`, `*.gen.go`, dbt `target/`), `.venv/`, or vendored deps.
- Never read or print secrets: `.env`, kubeconfig, MinIO/ClickHouse credentials.
- Deployment is GitOps: CI builds images; tags are bumped in `homelab-ops`. Never
  suggest `kubectl apply` or manual deploys from this repo.
- Run `ruff format && ruff check && mypy` (Python) before claiming done; tests via `pytest`.
- Plan mode for any change touching >1 file. Show the plan, wait for "execute".
- When I correct a mistake, append it to @.claude/memory/defects.md before continuing.
- All Kafka-bound and ClickHouse-bound data uses the typed models in the schemas
  package — no ad-hoc dicts crossing component boundaries.

## Imported context
- Stack & versions: @.claude/memory/tech-stack.md
- Standards & layout: @.claude/memory/standards.md
- Known defects (READ THIS — pre-loaded with platform gotchas): @.claude/memory/defects.md

## Workflow
- Session start: read @_state.md, summarize, confirm next step. Session end: update it.
- Big features: phased outputs in `.claude/outputs/<feature>/<phase>.md`.
- Noisy research (reading large files, comparing libs) goes to a subagent; review diffs
  with the `reviewer` subagent before commit.

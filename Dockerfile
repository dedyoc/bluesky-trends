FROM python:3.12-slim-bookworm

COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

ENV PYTHONUNBUFFERED=1 \
    UV_SYSTEM_PYTHON=1

WORKDIR /app

COPY pyproject.toml uv.lock ./

RUN uv sync --frozen --no-dev

COPY ingest/ /app/ingest/
COPY schemas/ /app/schemas/

# --no-dev: the image was built with `uv sync --frozen --no-dev`; without this flag
# `uv run` re-syncs the dev group (mypy/ruff/...) on every container start — slow, noisy,
# and it pulls dev tooling into the runtime image. --no-dev uses the prebuilt runtime env.
CMD ["uv", "run", "--no-dev", "python", "-m", "ingest.main"]

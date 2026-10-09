# syntax=docker/dockerfile:1

FROM ghcr.io/astral-sh/uv:python3.13-bookworm-slim AS base

WORKDIR /app

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    PYTHONUNBUFFERED=1 \
    PATH="/app/.venv/bin:$PATH"

COPY pyproject.toml uv.lock README.md alembic.ini ./
COPY src ./src
COPY alembic ./alembic
COPY openapi ./openapi

FROM base AS runtime

RUN uv sync --frozen --no-dev --no-editable \
    && useradd --system --create-home --uid 10001 --shell /usr/sbin/nologin queue \
    && chown -R queue:queue /app

# Copy as root before USER so --chmod applies; queue (10001) can execute.
COPY --chmod=0755 docker/entrypoint.sh /app/entrypoint.sh

USER queue
STOPSIGNAL SIGTERM
# Exec-form entrypoint so the role process receives SIGTERM (via Compose init).
ENTRYPOINT ["/app/entrypoint.sh"]
CMD ["--help"]

FROM base AS dev

RUN uv sync --frozen --group dev

CMD ["python", "-m", "queue_service"]

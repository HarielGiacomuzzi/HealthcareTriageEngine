# syntax=docker/dockerfile:1

FROM python:3.12-slim AS builder
COPY --from=ghcr.io/astral-sh/uv:0.12.4 /uv /usr/local/bin/uv
ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PROJECT_ENVIRONMENT=/opt/venv
WORKDIR /app
# Dependency layer first so source edits do not re-resolve the world.
COPY pyproject.toml uv.lock README.md ./
RUN uv sync --frozen --no-dev --no-install-project
ARG SPACY_MODEL=en_core_web_lg
RUN VIRTUAL_ENV=/opt/venv /opt/venv/bin/python -m spacy download ${SPACY_MODEL}
COPY src/ ./src/
# --inexact: the final sync must not prune the spaCy model installed above — it lives
# outside uv.lock, and a plain `uv sync` treats it as extraneous and removes it.
RUN uv sync --frozen --no-dev --no-editable --inexact

FROM python:3.12-slim AS runtime
ENV PATH="/opt/venv/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1
RUN useradd --create-home --uid 1000 ecet
COPY --from=builder /opt/venv /opt/venv
# Fail the build, not the container start, if the model didn't survive into this image.
RUN python -c "import en_core_web_lg"
USER ecet
WORKDIR /app
COPY alembic.ini ./
COPY migrations/ ./migrations/
EXPOSE 8000
ENTRYPOINT ["ecet"]
CMD ["api"]

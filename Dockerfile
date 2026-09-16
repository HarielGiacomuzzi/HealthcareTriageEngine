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
# Pinned: an unversioned `spacy download` takes the newest release compatible with the
# installed spaCy, so two builds could redact differently. 3.8.0 matches spaCy 3.8.x in
# uv.lock. Keep in step with the Makefile and ci.yml (tests/unit/test_spacy_pin.py).
ARG SPACY_MODEL_VERSION=3.8.0
RUN VIRTUAL_ENV=/opt/venv /opt/venv/bin/python -m spacy download \
    ${SPACY_MODEL}-${SPACY_MODEL_VERSION} --direct
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
ARG SPACY_MODEL=en_core_web_lg
RUN python -c "import ${SPACY_MODEL}"
USER ecet
WORKDIR /app
COPY alembic.ini ./
COPY migrations/ ./migrations/
EXPOSE 8000
ENTRYPOINT ["ecet"]
CMD ["api"]

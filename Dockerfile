FROM python:3.12-slim-bookworm as base

FROM base as builder

RUN pip install poetry==1.8.2

ENV POETRY_VIRTUALENVS_IN_PROJECT=1 \
    POETRY_CACHE_DIR=/tmp/poetry_cache

WORKDIR /app

COPY README.md pyproject.toml poetry.lock /app
COPY celerymon /app/celerymon

RUN poetry install --without dev && rm -rf $POETRY_CACHE_DIR
RUN find /app

FROM base as runtime

ENV PROMETHEUS_DISABLE_CREATED_SERIES=true \
    VIRTUAL_ENV=/app/.venv \
    PATH="/app/.venv/bin:$PATH"

COPY --from=builder /app /app

ENTRYPOINT ["celerymon"]

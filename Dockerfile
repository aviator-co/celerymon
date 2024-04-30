FROM python:3.12-bookworm as builder
WORKDIR /app
ENV RYE_HOME="/opt/rye"
ENV UV_CACHE_DIR="/opt/uv_cache"
ENV PATH="$RYE_HOME/shims:$PATH"

RUN curl -sSf https://rye-up.com/get | RYE_NO_AUTO_INSTALL=1 RYE_INSTALL_OPTION="--yes" bash
COPY . .
RUN --mount=type=cache,target=/opt/uv_cache rye build --wheel --clean

FROM python:3.12-slim-bookworm as runtime

RUN --mount=type=bind,source=./requirements.lock,target=/deps/requirements.lock PYTHONDONTWRITEBYTECODE=1 grep -v "-e file:." /deps/requirements.lock | pip install --no-cache-dir -r /dev/stdin
RUN --mount=type=bind,from=builder,source=/app/dist,target=/dist PYTHONDONTWRITEBYTECODE=1 pip install --no-cache-dir /dist/*.whl

ENV PROMETHEUS_DISABLE_CREATED_SERIES=true
ENTRYPOINT ["celerymon"]

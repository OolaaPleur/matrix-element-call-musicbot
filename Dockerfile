FROM docker.io/library/node:26-bookworm-slim

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y --no-install-recommends \
    ffmpeg \
    python3 \
    python3-venv \
    procps \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

ENV VIRTUAL_ENV=/opt/venv
ENV PATH="/opt/venv/bin:$PATH"

RUN python3 -m venv /opt/venv

# requirements.txt installs matrix-bot-common editable from a sibling checkout
# (host setup); the image takes it from GitHub instead.
ARG COMMON_URL=https://github.com/OolaaPleur/matrix-element-call-common/archive/refs/heads/master.tar.gz
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir --upgrade pip \
    && grep -v '^-e ' /app/requirements.txt > /tmp/requirements.txt \
    && pip install --no-cache-dir -r /tmp/requirements.txt "matrix-bot-common @ ${COMMON_URL}" \
    && pip install --no-cache-dir -U yt-dlp

COPY --from=docker.io/oven/bun:1.4.3-debian /usr/local/bin/bun /usr/local/bin/bun
COPY call_worker/package.json call_worker/bun.lock /app/call_worker/
COPY call_worker/patches /app/call_worker/patches
RUN bun install --cwd /app/call_worker --frozen-lockfile --production \
    && python3 /app/call_worker/patches/apply-pr768.py

COPY . /app

RUN useradd -m -u 10001 musicbot \
    && mkdir -p /app/logs /app/data /app/cache \
    && chown -R musicbot:musicbot /app

USER musicbot

ENV PYTHONUNBUFFERED=1
# Tells scripts/setup.py how to say "start the bot".
ENV BOT_IN_DOCKER=1

HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
  CMD pgrep -f "python3 main.py" > /dev/null || exit 1

CMD ["python3", "main.py"]

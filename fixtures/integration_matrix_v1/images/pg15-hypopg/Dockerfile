FROM postgres:15

RUN apt-get update \
    && apt-get install -y --no-install-recommends postgresql-15-hypopg \
    && rm -rf /var/lib/apt/lists/*

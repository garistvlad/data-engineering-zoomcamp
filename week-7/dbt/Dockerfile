FROM python:3.11

RUN apt-get update \
  && apt-get dist-upgrade -y \
  && apt-get install -y --no-install-recommends \
    git \
    ssh-client \
    software-properties-common \
    make \
    build-essential \
    ca-certificates \
    libpq-dev \
  && apt-get clean \
  && rm -rf \
    /var/lib/apt/lists/* \
    /tmp/* \
    /var/tmp/*

# Env vars
ENV PYTHONIOENCODING=utf-8
ENV LANG=C.UTF-8

# Set docker basics
WORKDIR /usr/app/dbt/
COPY . /usr/app/dbt/

# Update python
RUN python -m pip install --upgrade pip setuptools wheel --no-cache-dir

# install DBT & Adapters
RUN python -m pip install --no-cache-dir dbt-core dbt-postgres dbt-bigquery

# Add connection to BigQuery
RUN mkdir /root/.dbt && cp ./profiles.yml /root/.dbt/

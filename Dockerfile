FROM astrocrpublic.azurecr.io/runtime:3.1-7

SHELL ["/bin/bash", "-o", "pipefail", "-c"]

USER astro 

RUN pip install --no-cache-dir -e /usr/local/airflow 

RUN pip install --no-cache-dir "awscli"

RUN pip install --no-cache-dir "awscli-local"


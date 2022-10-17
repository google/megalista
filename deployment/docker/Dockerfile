FROM python:3.9-slim

# pre requirements
RUN apt-get update
RUN apt-get install -y curl build-essential

# Worker
RUN adduser --gecos [] --disabled-password worker

# copy files
WORKDIR /app
COPY --chown=worker:worker megalista_dataflow megalista_dataflow
COPY --chown=worker:worker deployment/docker/entrypoint.sh .
COPY --chown=worker:worker deployment/docker/service-account-file.json .

# pip
USER worker
RUN python -m ensurepip --upgrade
RUN pip install --upgrade pip setuptools wheel
RUN pip install --user --no-warn-script-location -r megalista_dataflow/requirements.txt

# set entrypoint
RUN ["chmod", "+x", "entrypoint.sh"]
ENTRYPOINT [ "/app/entrypoint.sh" ]


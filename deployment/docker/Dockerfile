FROM amazonlinux:latest

# Python
RUN yum install -y python37 shadow-utils
RUN rm /usr/bin/python
RUN ln -s /usr/bin/python3 /usr/bin/python

# pip
RUN curl -O https://bootstrap.pypa.io/get-pip.py
RUN python get-pip.py
RUN python -m ensurepip --upgrade

# Worker
RUN adduser --user-group worker
ENV PATH="/home/worker/.local/bin:${PATH}"

# copy files
WORKDIR /app
COPY --chown=worker:worker megalista_dataflow megalista_dataflow
USER worker
RUN pip install --upgrade pip setuptools wheel
RUN pip install --user --no-warn-script-location -r megalista_dataflow/requirements.txt

COPY --chown=worker:worker deployment/docker/entrypoint.sh .
COPY --chown=worker:worker deployment/docker/service-account-file.json .
RUN ["chmod", "+x", "entrypoint.sh"]
ENTRYPOINT [ "/app/entrypoint.sh" ]

FROM amazonlinux:latest

# Tools
RUN yum install -y shadow-utils curl git openssl11-devel tar gzip gcc make bzip2-devel bzip2-libs ncurses-devel ncurses-lib libffi libffi-devel readline-devel sqlite-devel pyliblzma xz xz-devel xz-libs

# Worker
RUN adduser --user-group worker
ENV PATH="/home/worker/.local/bin:${PATH}"

# Copy files
RUN mkdir /app
RUN chown -R worker:worker /app
USER worker
WORKDIR /app
COPY --chown=worker:worker megalista_dataflow megalista_dataflow

# Python - virtual env
RUN curl https://pyenv.run | bash
RUN /home/worker/.pyenv/bin/pyenv install 3.9.9
RUN /home/worker/.pyenv/bin/pyenv virtualenv 3.9.9 virtual_env
RUN mv /home/worker/.pyenv/versions/virtual_env /app
ENV VIRTUAL_ENV=/app/virtual_env
ENV PATH="$VIRTUAL_ENV/bin:$PATH" 
RUN env
RUN python --version
RUN python -m ensurepip --upgrade
RUN pip install --upgrade pip setuptools wheel

# Install requirements
RUN pip install --no-warn-script-location -r megalista_dataflow/requirements.txt

COPY --chown=worker:worker deployment/docker/entrypoint.sh .
COPY --chown=worker:worker deployment/docker/service-account-file.json ./megalista_dataflow/

RUN ["chmod", "+x", "entrypoint.sh"]
ENTRYPOINT [ "/app/entrypoint.sh" ]

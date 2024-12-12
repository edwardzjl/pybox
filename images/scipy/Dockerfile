FROM python:3.13-slim

ARG NB_USER="jovyan"
ARG NB_UID="1000"
ARG NB_GID="100"
ENV HOME=/home/$NB_USER

RUN pip install --no-cache-dir \
  ipykernel \
  ipython

# Packages are carefully selected from <https://github.com/jupyter/docker-stacks/blob/main/images/scipy-notebook/Dockerfile>
# and its base images

RUN apt-get update \
  && apt-get install -y --no-install-recommends \
  libopenblas-dev \
  # - Add necessary fonts for matplotlib/seaborn
  #   See https://github.com/jupyter/docker-stacks/pull/380 for details
  fonts-liberation \
  && apt-get clean && rm -rf /var/lib/apt/lists /var/cache/apt/archives

COPY requirements.txt .
RUN pip install --no-cache-dir \
  -r requirements.txt

ADD https://raw.githubusercontent.com/jupyter-server/enterprise_gateway/refs/heads/main/etc/kernel-launchers/bootstrap/bootstrap-kernel.sh /usr/local/bin/bootstrap-kernel.sh
RUN chmod a+x /usr/local/bin/bootstrap-kernel.sh
ADD https://raw.githubusercontent.com/jupyter-server/enterprise_gateway/refs/heads/main/etc/kernel-launchers/python/scripts/launch_ipykernel.py /usr/local/bin/kernel-launchers/python/scripts/launch_ipykernel.py

RUN useradd --create-home --no-log-init --no-user-group --uid ${NB_UID} ${NB_USER}

USER $NB_UID

CMD /usr/local/bin/bootstrap-kernel.sh

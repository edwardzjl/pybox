FROM python:3.13-slim

ARG NB_USER="jovyan"
ARG NB_UID="1000"
ARG NB_GID="100"
ENV HOME /home/$NB_USER

RUN pip install --no-cache-dir \
  ipykernel \
  ipython

RUN useradd --create-home --no-log-init --no-user-group --uid ${NB_UID} ${NB_USER}

USER $NB_UID

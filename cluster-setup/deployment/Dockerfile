ARG PYTHON_VERSION="bookworm"

FROM python:${PYTHON_VERSION} AS base

RUN apt update -y && apt upgrade -y

RUN pip install --upgrade pip

RUN pip install pyyaml

CMD ["/bin/bash", "-l"]

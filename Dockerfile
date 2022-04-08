ARG IMAGE_PYTHON_VERSION="3.10"

FROM python:${IMAGE_PYTHON_VERSION}-slim

# install system deps
RUN apt update && apt install -y --no-install-recommends curl

# install pdm
RUN curl -sSL https://raw.githubusercontent.com/pdm-project/pdm/main/install-pdm.py | python3 -
ENV PATH="/root/.local/bin:$PATH"

# copy project into container
ENV HOME="/root"
WORKDIR $HOME/app
COPY . $HOME/app

# install project dependencies
RUN pdm install --prod

# add pdm directories to PATH envars
ARG IMAGE_PYTHON_VERSION
ENV PYTHONPATH="/root/.local/share/pdm/venv/lib/python${IMAGE_PYTHON_VERSION}/site-packages/pdm/pep582:/root/app/__pypackages__/${IMAGE_PYTHON_VERSION}/lib"
ENV PATH="/root/app/__pypackages__/${IMAGE_PYTHON_VERSION}/bin:$PATH"

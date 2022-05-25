ARG IMAGE_PYTHON_VERSION="3.10"

# Debian Bullseye
FROM python:${IMAGE_PYTHON_VERSION}-slim

# install system deps
RUN apt update && apt install -y --no-install-recommends curl gnupg build-essential

# install Microsoft ODBC driver for SQL Server
# https://docs.microsoft.com/en-us/sql/connect/odbc/linux-mac/installing-the-microsoft-odbc-driver-for-sql-server?view=sql-server-ver16#debian18
RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
RUN curl https://packages.microsoft.com/config/debian/11/prod.list > /etc/apt/sources.list.d/mssql-release.list
RUN apt update
RUN ACCEPT_EULA=Y apt install -y msodbcsql18 unixodbc-dev

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

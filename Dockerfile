FROM python:latest

# copy folder contents into container
COPY . .

# install pdm
RUN curl -sSL https://raw.githubusercontent.com/pdm-project/pdm/main/install-pdm.py | python3 -
RUN eval "$(pdm --pep582)"

RUN pdm install --prod

# WORKDIR /usr/src/app
# COPY requirements.txt ./
# RUN pip install --no-cache-dir -r requirements.txt



# CMD [ "python", "./your-daemon-or-script.py" ]

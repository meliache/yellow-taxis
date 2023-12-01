FROM python:3.11

WORKDIR /src
ADD . yellow-taxis
WORKDIR /src/yellow-taxis


# install curl which is needed to download datasets from server
RUN <<EOF
	apt-get -y update
	apt-get -y install curl
EOF


# Install PDM project and dependency manager for Python https://github.com/pdm-project/pdm
RUN pip install --no-cache --upgrade pip
RUN pip install --no-cache pdm

# Install my package without developer dependencies
RUN pdm install --fail-fast --production

# Activate virtual environment
RUN . .venv/bin/activate

# By default enable the central scheduler
EXPOSE 8887
CMD ["pdm" "run" "luigid", "--port", "8887"]

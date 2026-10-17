FROM python:3.11-slim-bookworm

# Spark needs Java
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# the code and data are mounted from the host by docker-compose (see docker-compose.yml)
CMD ["bash"]

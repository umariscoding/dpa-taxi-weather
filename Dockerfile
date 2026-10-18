FROM python:3.11-slim-bookworm

# Spark needs Java
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# only show errors from spark, not the long list of warnings at the start
RUN mkdir -p /usr/local/lib/python3.11/site-packages/pyspark/conf \
    && printf 'rootLogger.level = error\nrootLogger.appenderRef.stdout.ref = console\nappender.console.type = Console\nappender.console.name = console\nappender.console.target = SYSTEM_ERR\nappender.console.layout.type = PatternLayout\nappender.console.layout.pattern = %%d{yy/MM/dd HH:mm:ss} %%p %%c{1}: %%m%%n\n' \
    > /usr/local/lib/python3.11/site-packages/pyspark/conf/log4j2.properties

# the code and data are mounted from the host by docker-compose (see docker-compose.yml)
CMD ["bash"]

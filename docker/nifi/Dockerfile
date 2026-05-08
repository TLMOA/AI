FROM apache/nifi:latest

USER root

# Install Python3 and pip, then install pymysql
RUN apt-get update \
    && apt-get install -y python3 python3-pip \
    && pip3 install pymysql \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Ensure NiFi user owns mounted data dirs
RUN mkdir -p /opt/nifi/nifi-current/data/iot && chown -R nifi:nifi /opt/nifi/nifi-current/data/iot

USER nifi

# Entrypoint uses upstream NiFi image entrypoint

# Provide default working dir
WORKDIR /opt/nifi/nifi-current

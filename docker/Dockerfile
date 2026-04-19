FROM ubuntu:24.04

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential cmake clang libc++-dev libc++abi-dev \
    git mosquitto mosquitto-clients \
    libpaho-mqtt-dev libpaho-mqttpp-dev \
    openssl libssl-dev libgtest-dev \
    && rm -rf /var/lib/apt/lists/*

COPY generate-certs.sh /usr/local/bin/generate-certs.sh
COPY setup-auth.sh /usr/local/bin/setup-auth.sh
COPY mosquitto.conf /etc/mosquitto/conf.d/custom.conf
COPY entrypoint.sh /usr/local/bin/entrypoint.sh

RUN chmod +x /usr/local/bin/generate-certs.sh \
    /usr/local/bin/setup-auth.sh \
    /usr/local/bin/entrypoint.sh

EXPOSE 1883 8883

ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
CMD ["bash"]

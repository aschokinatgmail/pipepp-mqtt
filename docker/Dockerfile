FROM ubuntu:24.04

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y \
    ca-certificates \
    cmake \
    clang \
    libc++-dev \
    libc++abi-dev \
    git \
    mosquitto \
    mosquitto-clients \
    libpaho-mqtt-dev \
    libpaho-mqttpp-dev \
    libssl-dev \
    libgtest-dev \
    openssl \
    && rm -rf /var/lib/apt/lists/*

RUN mkdir -p /run/mosquitto && chown mosquitto:mosquitto /run/mosquitto

COPY mosquitto.conf /etc/mosquitto/conf.d/custom.conf
COPY generate-certs.sh /generate-certs.sh
COPY setup-auth.sh /setup-auth.sh
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /generate-certs.sh /setup-auth.sh /entrypoint.sh

WORKDIR /build

ENTRYPOINT ["/entrypoint.sh"]
CMD ["bash"]

FROM ubuntu:24.04

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential cmake clang libc++-dev libc++abi-dev \
    git mosquitto mosquitto-clients ca-certificates \
    libpaho-mqtt-dev \
    openssl libssl-dev libgtest-dev gcovr \
    && rm -rf /var/lib/apt/lists/*

RUN git clone --depth 1 --branch v1.5.0 https://github.com/eclipse/paho.mqtt.cpp.git /tmp/paho-cpp \
    && cmake -S /tmp/paho-cpp -B /tmp/paho-cpp/build \
        -DCMAKE_BUILD_TYPE=Release \
        -DPAHO_BUILD_SHARED=ON \
        -DPAHO_BUILD_STATIC=OFF \
        -DPAHO_WITH_SSL=ON \
    && cmake --build /tmp/paho-cpp/build -j$(nproc) \
    && cmake --install /tmp/paho-cpp/build \
    && ldconfig \
    && rm -rf /tmp/paho-cpp

RUN mkdir -p /run/mosquitto && chown mosquitto:mosquitto /run/mosquitto

COPY mosquitto.conf /etc/mosquitto/conf.d/custom.conf
COPY generate-certs.sh /generate-certs.sh
COPY setup-auth.sh /setup-auth.sh
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /generate-certs.sh /setup-auth.sh /entrypoint.sh

WORKDIR /build

ENTRYPOINT ["/entrypoint.sh"]
CMD ["bash"]

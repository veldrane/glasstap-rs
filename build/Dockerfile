FROM quay.io/rockylinux/rockylinux:9-minimal

LABEL MANTAINER "-veldrane"

COPY target/x86_64-unknown-linux-musl/release/glass-tap /glass-tap

RUN microdnf install -y net-tools iproute iputils bash \
    && chmod +x /glass-tap 

ENTRYPOINT ["/glass-tap"]

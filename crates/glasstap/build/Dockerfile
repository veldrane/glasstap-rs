FROM quay.io/rockylinux/rockylinux:9-minimal

LABEL MANTAINER "-veldrane"

COPY target/x86_64-unknown-linux-musl/release/glasstap /glasstap

RUN microdnf install -y net-tools iproute iputils bash \
    && chmod +x /glasstap 

ENTRYPOINT ["/glasstap"]

FROM fluent/fluent-bit:0.11
USER root
COPY etc /fluent-bit/etc/
COPY fluent-bit-go.so /fluent-bit/
CMD ["/fluent-bit/bin/fluent-bit", "-c", "/fluent-bit/etc/fluent-bit.conf", "-e", "/fluent-bit/fluent-bit-go.so"]
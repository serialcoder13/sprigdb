# tc, ip and ping for the three-DC layout's latency sidecar (netem.sh) and
# for `3dc.sh latency`.
FROM alpine:3.20
RUN apk add --no-cache iproute2 iproute2-tc
COPY netem.sh /usr/local/bin/netem.sh
ENTRYPOINT ["/bin/sh", "/usr/local/bin/netem.sh"]

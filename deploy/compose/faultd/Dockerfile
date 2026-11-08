# faultd (chapter 46, "Fault injection"): iptables partitions and netem
# delays inside other containers' network namespaces. Runs privileged in
# the host PID namespace; sprig-testkit drives it with `docker exec` and
# only ever targets its own sprig-sim-* containers.
FROM alpine:3.20
RUN apk add --no-cache iptables iproute2 iproute2-tc util-linux-misc
COPY faultd.sh /usr/local/bin/faultd.sh
ENTRYPOINT ["/bin/sh", "/usr/local/bin/faultd.sh"]
CMD ["idle"]

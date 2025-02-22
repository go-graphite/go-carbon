FROM golang:alpine AS build
ARG TARGETARCH
ARG GOCACHE=/tmp

RUN apk add --update git make bash gcc musl-dev

USER nobody:nogroup
WORKDIR /usr/local/src/go-carbon
COPY --chown=nobody:nogroup . .
RUN --network=none make clean
RUN make go-carbon
RUN <<EOT
if [ "${TARGETARCH:-unknown}" = "amd64" ]; then
  make run-test COMMAND="test -race" 
else
  make run-test COMMAND="test" || true
fi
EOT

FROM alpine:latest

RUN --network=none addgroup -S carbon && adduser -S carbon -G carbon \
  && mkdir -p /var/lib/graphite/whisper /etc/go-carbon/ \
  && chown -R carbon:carbon /var/lib/graphite/

COPY --chown=0:0 --chmod=755 --from=build /usr/local/src/go-carbon/go-carbon /usr/sbin/go-carbon
ADD go-carbon.docker.conf /etc/go-carbon/go-carbon.conf
ADD deploy/storage*.conf /etc/go-carbon/
RUN --network=none /usr/sbin/go-carbon -config-print-default > /etc/go-carbon/go-carbon.default.conf

USER carbon
ENTRYPOINT ["/usr/sbin/go-carbon"]
CMD ["-config", "/etc/go-carbon/go-carbon.conf"]

EXPOSE 2003/tcp 2003/udp 8080
VOLUME /var/lib/graphite /etc/go-carbon

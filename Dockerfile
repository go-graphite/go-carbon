FROM busybox

RUN mkdir -p /data/graphite/whisper/
ADD go-carbon /usr/sbin
ADD conf-examples/* /data/graphite/
CMD ["go-carbon", "-config", "/data/graphite/carbon.conf"]

EXPOSE 2003 2004 7002 7007 2003/udp
VOLUME /data/graphite/

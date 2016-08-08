FROM osrg/ryu

RUN \
  apt-get update && \
  apt-get install -qy --no-install-recommends python-yaml

COPY ./ /srv/valve

VOLUME ["/etc/valve/"]

RUN \
  ln -s /etc/valve/valve.yaml /srv/valve/

WORKDIR /srv/valve

EXPOSE 6653 8080

CMD ["ryu-manager", "--ofp-tcp-listen-port=6653", "valve.py"]

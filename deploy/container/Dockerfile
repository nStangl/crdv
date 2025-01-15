FROM ubuntu:22.04
RUN apt update; apt install sudo vim -y
COPY ./ /
RUN sed -i'' "s/sudo systemctl restart postgresql/service postgresql restart/g" deploy/install_postgres.sh
RUN cd deploy; chmod +x *.sh;  ./install_postgres.sh; ./install_extensions.sh; ./install_pg_crdt.sh; ./install_riak.sh; ./install_python.sh; ./install_metrics_server.sh;

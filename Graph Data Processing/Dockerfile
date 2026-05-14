# Base image: ubuntu:22.04
FROM ubuntu:22.04

# ARGs
# https://docs.docker.com/engine/reference/builder/#understand-how-arg-and-from-interact
ARG TARGETPLATFORM=linux/amd64,linux/arm64
ARG DEBIAN_FRONTEND=noninteractive

# neo4j 2025.08.0 installation (match GDS v2.21.0) and some cleanup
RUN apt-get update && \
    apt-get install -y wget gnupg software-properties-common && \
    wget -O - https://debian.neo4j.com/neotechnology.gpg.key | apt-key add - && \
    echo 'deb https://debian.neo4j.com stable latest' > /etc/apt/sources.list.d/neo4j.list && \
    add-apt-repository universe && \
    apt-get update && \
    apt-get install -y nano unzip neo4j=1:2025.08.0 python3-pip && \
    apt-get autoremove -y && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

RUN neo4j-admin dbms set-initial-password graphprocessing

# TODO: Complete the Dockerfile

# 1. Create working directory: /cse511/
RUN mkdir -p /cse511

# 2. Download the March 2022 NYC Taxi dataset
RUN cd /cse511 && \
    wget -q https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2022-03.parquet

# 3. Copy data_loader.py 
COPY data_loader.py /cse511/data_loader.py

# 4. Install Java (OpenJDK 21)
RUN apt-get update && \
    apt-get install -y openjdk-21-jdk && \
    apt-get clean

# Set JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64

# 5. Upgrade pip and install Python packages
RUN python3 -m pip install --upgrade pip
RUN pip3 install neo4j pandas pyarrow

# 6. Download and install GDS plugin 
RUN mkdir -p /var/lib/neo4j/plugins && \
    wget -q -O /var/lib/neo4j/plugins/neo4j-graph-data-science-2.21.0.jar \
      https://github.com/neo4j/graph-data-science/releases/download/2.21.0/neo4j-graph-data-science-2.21.0.jar

# 7. Configure Neo4j
RUN echo 'dbms.default_listen_address=0.0.0.0' >> /etc/neo4j/neo4j.conf && \
    echo 'dbms.default_advertised_address=localhost' >> /etc/neo4j/neo4j.conf && \
    echo 'dbms.connector.bolt.listen_address=:7687' >> /etc/neo4j/neo4j.conf && \
    echo 'dbms.connector.http.listen_address=:7474' >> /etc/neo4j/neo4j.conf && \
    echo 'dbms.security.procedures.unrestricted=gds.*,apoc.*' >> /etc/neo4j/neo4j.conf && \
    echo 'dbms.security.procedures.allowlist=gds.*,apoc.*' >> /etc/neo4j/neo4j.conf

# Run the data loader script
RUN chmod +x /cse511/data_loader.py

# Expose neo4j ports
EXPOSE 7474 7687

# Start neo4j service and show the logs on container run
CMD ["/bin/bash", "-c", "neo4j start && sleep 30 && cd /cse511 && python3 data_loader.py && tail -f /dev/null"]
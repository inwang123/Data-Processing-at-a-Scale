FROM confluentinc/cp-kafka-connect:7.3.0

# Become root so we can install the Neo4j connector
USER root

# Install Neo4j Kafka Sink Connector plugin
RUN confluent-hub install --no-prompt neo4j/kafka-connect-neo4j:latest

# Copy init script and connector config into the image
COPY init.sh /init.sh
COPY sink.neo4j.json /sink.neo4j.json

# No chmod needed; we will run it via bash
CMD ["bash", "/init.sh"]

FROM folioci/alpine-jre-openjdk11:latest

ENV VERTICLE_FILE mod-oai-pmh-fat.jar

# Set the location of the verticles
ENV VERTICLE_HOME /usr/verticles

# Copy your fat jar to the container
COPY target/${VERTICLE_FILE} ${VERTICLE_HOME}/${VERTICLE_FILE}

# Add JMX exporter and config
RUN mkdir -p jmx_exporter &&\
    wget -P jmx_exporter https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/0.17.2/jmx_prometheus_javaagent-0.17.2.jar
COPY ./prometheus-jmx-config.yaml jmx_exporter/

# Expose this port locally in the container.
EXPOSE 8081 9991

FROM folioci/alpine-jre-openjdk11:latest

ENV VERTICLE_FILE mod-oai-pmh-fat.jar

# Set the location of the verticles
ENV VERTICLE_HOME /usr/verticles
ENV DOCKER_API_VERSION=1.39

# Copy your fat jar to the container
COPY target/${VERTICLE_FILE} ${VERTICLE_HOME}/${VERTICLE_FILE}

# Expose this port locally in the container.
EXPOSE 8081

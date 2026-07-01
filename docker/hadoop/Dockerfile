FROM eclipse-temurin:17-jdk-jammy

ARG HADOOP_VERSION=3.3.6
ENV HADOOP_HOME=/opt/hadoop
ENV JAVA_HOME=/opt/java/openjdk
ENV PATH=${PATH}:${HADOOP_HOME}/bin:${HADOOP_HOME}/sbin
ENV HADOOP_CONF_DIR=${HADOOP_HOME}/etc/hadoop

RUN apt-get update \
    && apt-get install -y --no-install-recommends bash curl procps netcat-openbsd openssh-client \
    && rm -rf /var/lib/apt/lists/*

RUN set -eux; \
    for attempt in 1 2 3 4 5; do \
      url="https://dlcdn.apache.org/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz"; \
      if [ "$attempt" -gt 2 ]; then \
        url="https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz"; \
      fi; \
      curl --fail --location --silent --show-error --connect-timeout 30 --max-time 600 --continue-at - \
        "$url" -o /tmp/hadoop.tar.gz && break; \
      test "$attempt" -lt 5; \
      sleep 5; \
    done; \
    tar -xzf /tmp/hadoop.tar.gz -C /opt \
    && mv "/opt/hadoop-${HADOOP_VERSION}" "${HADOOP_HOME}" \
    && rm /tmp/hadoop.tar.gz \
    && mkdir -p /data/name /data/data /data/yarn/local /data/yarn/log

COPY docker/hadoop/conf/ ${HADOOP_CONF_DIR}/
COPY docker/hadoop/entrypoint.sh /usr/local/bin/hadoop-role

RUN chmod +x /usr/local/bin/hadoop-role \
    && printf '\nexport JAVA_HOME=%s\n' "${JAVA_HOME}" >> "${HADOOP_CONF_DIR}/hadoop-env.sh" \
    && printf '\nexport JAVA_HOME=%s\n' "${JAVA_HOME}" >> "${HADOOP_CONF_DIR}/yarn-env.sh" \
    && printf '\nexport HADOOP_OPTS="--add-opens java.base/java.lang=ALL-UNNAMED"\n' >> "${HADOOP_CONF_DIR}/hadoop-env.sh" \
    && printf '\nexport YARN_OPTS="--add-opens java.base/java.lang=ALL-UNNAMED"\n' >> "${HADOOP_CONF_DIR}/yarn-env.sh"

ENTRYPOINT ["/usr/local/bin/hadoop-role"]

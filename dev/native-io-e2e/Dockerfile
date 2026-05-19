ARG SPARK_RUNTIME_IMAGE=apache/spark:3.5.8-scala2.12-java11-python3-ubuntu

FROM ${SPARK_RUNTIME_IMAGE} AS spark-runtime

FROM spark-runtime AS spark-layout
USER root
RUN set -eux; \
    mkdir -p \
      /spark/base \
      /spark/jars-a-b \
      /spark/jars-c-g \
      /spark/jars-hadoop-api \
      /spark/jars-hadoop-runtime \
      /spark/jars-h-other \
      /spark/jars-i-l \
      /spark/jars-m-q \
      /spark/jars-r-other \
      /spark/jars-scala \
      /spark/jars-spark-core \
      /spark/jars-spark-other \
      /spark/jars-s-other \
      /spark/jars-t-z; \
    cp -a /opt/spark/RELEASE /spark/base/; \
    cp -a /opt/spark/R /spark/base/; \
    cp -a /opt/spark/bin /spark/base/; \
    cp -a /opt/spark/data /spark/base/; \
    cp -a /opt/spark/examples /spark/base/; \
    cp -a /opt/spark/python /spark/base/; \
    cp -a /opt/spark/sbin /spark/base/; \
    cp -a /opt/spark/tests /spark/base/; \
    cp -a /opt/spark/work-dir /spark/base/; \
    for jar in /opt/spark/jars/*.jar; do \
      name="$(basename "${jar}")"; \
      case "${name}" in \
        rocksdbjni-*) ;; \
        [a-bA-B]*) cp -a "${jar}" /spark/jars-a-b/ ;; \
        [c-gC-G]*) cp -a "${jar}" /spark/jars-c-g/ ;; \
        hadoop-client-api-*) cp -a "${jar}" /spark/jars-hadoop-api/ ;; \
        hadoop-client-runtime-*) cp -a "${jar}" /spark/jars-hadoop-runtime/ ;; \
        [hH]*) cp -a "${jar}" /spark/jars-h-other/ ;; \
        [i-lI-L]*) cp -a "${jar}" /spark/jars-i-l/ ;; \
        [m-qM-Q]*) cp -a "${jar}" /spark/jars-m-q/ ;; \
        [rR]*) cp -a "${jar}" /spark/jars-r-other/ ;; \
        scala-*) cp -a "${jar}" /spark/jars-scala/ ;; \
        spark-catalyst_*|spark-core_*|spark-sql_*) cp -a "${jar}" /spark/jars-spark-core/ ;; \
        spark-*) cp -a "${jar}" /spark/jars-spark-other/ ;; \
        [sS]*) cp -a "${jar}" /spark/jars-s-other/ ;; \
        *) cp -a "${jar}" /spark/jars-t-z/ ;; \
      esac; \
    done

FROM eclipse-temurin:8-jdk-jammy AS jdk-runtime
FROM rust:1-bookworm

ENV JAVA_HOME=/opt/java/openjdk
ENV SPARK_HOME=/opt/spark
ENV MAVEN_VERSION=3.9.9
ENV MAVEN_HOME=/opt/maven
ENV CARGO_HOME=/usr/local/cargo
ENV RUSTUP_HOME=/usr/local/rustup
ENV PATH=${MAVEN_HOME}/bin:${CARGO_HOME}/bin:${SPARK_HOME}/bin:${JAVA_HOME}/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin

COPY --from=jdk-runtime /opt/java/openjdk /opt/java/openjdk
COPY --from=spark-layout /spark/base /opt/spark
COPY --from=spark-layout /spark/jars-a-b /opt/spark/jars
COPY --from=spark-layout /spark/jars-c-g /opt/spark/jars
COPY --from=spark-layout /spark/jars-hadoop-api /opt/spark/jars
COPY --from=spark-layout /spark/jars-hadoop-runtime /opt/spark/jars
COPY --from=spark-layout /spark/jars-h-other /opt/spark/jars
COPY --from=spark-layout /spark/jars-i-l /opt/spark/jars
COPY --from=spark-layout /spark/jars-m-q /opt/spark/jars
COPY --from=spark-layout /spark/jars-r-other /opt/spark/jars
COPY --from=spark-layout /spark/jars-scala /opt/spark/jars
COPY --from=spark-layout /spark/jars-spark-core /opt/spark/jars
COPY --from=spark-layout /spark/jars-spark-other /opt/spark/jars
COPY --from=spark-layout /spark/jars-s-other /opt/spark/jars
COPY --from=spark-layout /spark/jars-t-z /opt/spark/jars

COPY apache-maven-${MAVEN_VERSION}-bin.tar.gz /tmp/maven.tgz

RUN mkdir -p "${MAVEN_HOME}" \
    && tar -xzf /tmp/maven.tgz -C "${MAVEN_HOME}" --strip-components=1 \
    && rm -f /tmp/maven.tgz \
    && rustc --version \
    && cargo --version \
    && mvn -version \
    && java -version \
    && spark-submit --version

COPY container-entrypoint.sh /opt/paimon-nativeio-e2e/container-entrypoint.sh
RUN chmod +x /opt/paimon-nativeio-e2e/container-entrypoint.sh

WORKDIR /work/nativeio/paimon
ENTRYPOINT ["/opt/paimon-nativeio-e2e/container-entrypoint.sh"]

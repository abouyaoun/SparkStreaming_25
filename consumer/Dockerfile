FROM openjdk:11

# Installer Spark
RUN curl -fsSL https://archive.apache.org/dist/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3.tgz | tar -xz -C /opt && \
    ln -s /opt/spark-3.5.0-bin-hadoop3 /opt/spark

ENV PATH="/opt/spark/bin:$PATH"

WORKDIR /app
COPY target/scala-2.12/consumer-app-assembly-1.0.0.jar /app/

CMD ["/opt/spark/bin/spark-submit", "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.3.1", "--class", "com.example.consumer.ConsumerApp", "/app/consumer-app-assembly-1.0.0.jar"]
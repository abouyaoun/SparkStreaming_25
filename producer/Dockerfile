FROM openjdk:11-jre-slim

WORKDIR /app

COPY target/scala-2.12/producer-app-assembly-1.0.0-SNAPSHOT.jar /app/producer.jar

CMD ["java", "-jar", "producer.jar"]

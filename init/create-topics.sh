#!/bin/bash

KAFKA_TOPICS_CMD="/usr/bin/kafka-topics"
BOOTSTRAP_SERVER="kafka:9092"
TOPIC_NAME="my_topic"

echo "🔄 Attente que Kafka soit disponible sur $BOOTSTRAP_SERVER..."

# Essayer de lister les topics jusqu’à ce que Kafka réponde
until $KAFKA_TOPICS_CMD --bootstrap-server "$BOOTSTRAP_SERVER" --list >/dev/null 2>&1
do
  echo "⏳ Kafka pas encore prêt... nouvelle tentative dans 5s"
  sleep 5
done

echo "✅ Kafka est disponible. Création du topic si nécessaire..."

# Créer le topic (s’il n’existe pas)
$KAFKA_TOPICS_CMD --bootstrap-server "$BOOTSTRAP_SERVER" \
  --create --if-not-exists \
  --replication-factor 1 \
  --partitions 1 \
  --topic "$TOPIC_NAME"

echo "✅ Topic '$TOPIC_NAME' vérifié ou créé."
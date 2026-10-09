"""Kafka configuration for FinBank Phase 2."""

from __future__ import annotations

import os
from dataclasses import dataclass


DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092"
DEFAULT_TOPIC = "transactions"
DEFAULT_CONSUMER_GROUP = "finbank-transaction-consumer"


@dataclass(frozen=True)
class KafkaConfig:
    bootstrap_servers: str = DEFAULT_BOOTSTRAP_SERVERS
    topic: str = DEFAULT_TOPIC
    consumer_group: str = DEFAULT_CONSUMER_GROUP

    @classmethod
    def from_environment(cls) -> "KafkaConfig":
        bootstrap_servers = os.getenv(
            "KAFKA_BOOTSTRAP_SERVERS",
            DEFAULT_BOOTSTRAP_SERVERS,
        )
        topic = os.getenv(
            "KAFKA_TOPIC",
            DEFAULT_TOPIC,
        )
        consumer_group = os.getenv(
            "KAFKA_CONSUMER_GROUP",
            DEFAULT_CONSUMER_GROUP,
        )

        config = cls(
            bootstrap_servers=bootstrap_servers,
            topic=topic,
            consumer_group=consumer_group,
        )
        config.validate()
        return config

    def validate(self) -> None:
        if not self.bootstrap_servers.strip():
            raise ValueError("bootstrap_servers cannot be empty")
        if not self.topic.strip():
            raise ValueError("topic cannot be empty")
        if not self.consumer_group.strip():
            raise ValueError("consumer_group cannot be empty")

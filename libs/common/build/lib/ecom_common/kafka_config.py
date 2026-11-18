"""Kafka client configuration for Google Cloud Managed Service for Apache Kafka.

Managed Kafka listens on SASL_SSL:9092. With SASL/PLAIN the username is the
service-account e-mail and the password is the base64-encoded service-account
key JSON. For local development set KAFKA_SECURITY_PROTOCOL=PLAINTEXT.
"""
import os


def kafka_config(client_id: str, extra: dict | None = None) -> dict:
    conf = {
        "bootstrap.servers": os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092"),
        "client.id": client_id,
    }
    protocol = os.getenv("KAFKA_SECURITY_PROTOCOL", "SASL_SSL")
    conf["security.protocol"] = protocol
    if protocol.startswith("SASL"):
        conf.update(
            {
                "sasl.mechanisms": "PLAIN",
                "sasl.username": os.environ["KAFKA_SASL_USERNAME"],
                "sasl.password": os.environ["KAFKA_SASL_PASSWORD"],
                "ssl.ca.location": os.getenv("KAFKA_CA_LOCATION", "/etc/ssl/certs/ca-certificates.crt"),
            }
        )
    if extra:
        conf.update(extra)
    return conf


EVENTS_TOPIC = os.getenv("KAFKA_EVENTS_TOPIC", "shop-events")
RESULTS_TOPIC = os.getenv("KAFKA_RESULTS_TOPIC", "analytics-results")

import json
import os
import uuid

import paho.mqtt.client as mqtt
from kafka import KafkaProducer


MQTT_HOST = os.getenv("MQTT_HOST", "mosquitto")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_TOPIC = os.getenv("MQTT_TOPIC", "lambda26/red/llsw3/telemetria")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC_RED = os.getenv("KAFKA_TOPIC_RED", "red-telemetria")

# Mismo criterio que el puente de S7: no valida ni transforma, reenvía el
# payload tal cual llegó. Esquema y rango físico se validan solo en consumer_red.py.
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    key_serializer=lambda key: key.encode("utf-8"),
    value_serializer=lambda value: value,
)


def sensor_id_de(raw):
    try:
        event = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return "desconocido"
    if not isinstance(event, dict):
        return "desconocido"
    return str(event.get("sensorId") or "desconocido")


def on_connect(client, userdata, flags, reason_code, properties=None):
    print(json.dumps({
        "service": "uso-red",
        "component": "bridge",
        "mqttHost": MQTT_HOST,
        "mqttTopic": MQTT_TOPIC,
        "status": "connected" if reason_code == 0 else f"connect_failed:{reason_code}",
    }))
    client.subscribe(MQTT_TOPIC, qos=1)


def on_message(client, userdata, msg):
    sensor_id = sensor_id_de(msg.payload)
    metadata = producer.send(KAFKA_TOPIC_RED, key=sensor_id, value=msg.payload).get(timeout=10)
    print(json.dumps({
        "service": "uso-red",
        "component": "bridge",
        "mqttTopic": msg.topic,
        "kafkaTopic": metadata.topic,
        "partition": metadata.partition,
        "offset": metadata.offset,
        "sensorId": sensor_id,
        "status": "forwarded",
    }))


client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=f"uso-red-bridge-{uuid.uuid4().hex[:8]}")
client.on_connect = on_connect
client.on_message = on_message

print(json.dumps({
    "service": "uso-red",
    "component": "bridge",
    "mqttHost": MQTT_HOST,
    "mqttPort": MQTT_PORT,
    "kafkaBootstrapServers": KAFKA_BOOTSTRAP_SERVERS,
    "status": "starting",
}))

client.connect(MQTT_HOST, MQTT_PORT, keepalive=60)
client.loop_forever()

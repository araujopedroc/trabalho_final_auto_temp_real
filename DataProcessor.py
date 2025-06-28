import paho.mqtt.client as mqtt
from influxdb import InfluxDBClient
import json
from datetime import datetime
import time

# MQTT
MQTT_BROKER = 'localhost'
MQTT_PORT = 1883

# Tópicos de entrada
INPUT_TOPICS = [
    "iot/niveis/usina1",
    "iot/niveis/usina2",
    "iot/niveis/usina3"
]

# InfluxDB
INFLUX_HOST = 'localhost'
INFLUX_PORT = 8086
INFLUX_DB = 'Automacao'

# Limites para alertas
LIMITE_MIN = 1.1
LIMITE_MAX = 1.75

# Conexão com InfluxDB
influx = InfluxDBClient(host=INFLUX_HOST, port=INFLUX_PORT)
influx.switch_database(INFLUX_DB)

# Quando conecta ao broker
def on_connect(client, userdata, flags, rc):
    print(f"[MQTT] Conectado ao broker {MQTT_BROKER}:{MQTT_PORT}")
    for topic in INPUT_TOPICS:
        client.subscribe(topic)
        print(f"[MQTT] Inscrito no tópico: {topic}")

# Quando recebe mensagem
def on_message(client, userdata, msg):
    try:
        payload = msg.payload.decode()
        niveis = json.loads(payload)
        usina = msg.topic.split('/')[-1]

        print(f"\n📥 [RECEBIDO] Tópico: {msg.topic}")
        print(f"Dados recebidos: {niveis}")

        # Alertas e pontos para InfluxDB
        alertas = []
        pontos_influx = []

        for tanque, valor in niveis.items():
            if valor < LIMITE_MIN:
                alertas.append(f"⚠️ Nível BAIXO no {tanque} da {usina} ({valor:.2f} m)")
            elif valor > LIMITE_MAX:
                alertas.append(f"⚠️ Nível ALTO no {tanque} da {usina} ({valor:.2f} m)")

            estado = "normal"
            if valor < LIMITE_MIN:
                estado = "baixo"
            elif valor > LIMITE_MAX:
                estado = "alto"

            pontos_influx.append({
                "measurement": "estado_nivel",
                "tags": {
                    "usina": usina,
                    "tanque": tanque
                },
                "time": datetime.utcnow().isoformat(),
                "fields": {
                    "estado": estado  # string com 'baixo', 'normal' ou 'alto'
                }
            })
        # Envia para InfluxDB
        influx.write_points(pontos_influx)
        print("✅ Dados gravados no InfluxDB.")

        # Publica alertas no MQTT
        if alertas:
            alerta_msg = {
                "usina": usina,
                "alertas": alertas
            }
            alerta_topic = f"iot/alertas/{usina}"
            client.publish(alerta_topic, json.dumps(alerta_msg))
            print(f"📤 Alertas publicados em {alerta_topic}:\n{json.dumps(alerta_msg, indent=2)}")
        else:
            print("ℹ️ Níveis dentro da faixa segura.")

    except Exception as e:
        print(f"❌ Erro ao processar mensagem do tópico {msg.topic}: {e}")

# Programa principal
def main():
    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_message = on_message

    client.connect(MQTT_BROKER, MQTT_PORT, 60)
    client.loop_start()

    print("🚀 Processador de dados iniciado. Aguardando mensagens...\n")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n🛑 Encerrando...")
        client.loop_stop()
        client.disconnect()

if __name__ == "__main__":
    main()

from opcua import Client
import time
import json
import paho.mqtt.client as mqtt

# Configuração do servidor OPC UA
SERVER_URL = "opc.tcp://localhost:53530/OPCUA/SimulationServer"

# Configuração do MQTT Broker
MQTT_BROKER = "localhost"
MQTT_PORT = 1883

# Node IDs para todas as 3 usinas
node_ids_list = [
    { # Usina 1
        "h1": "ns=3;i=1008", "h2": "ns=3;i=1009", "h3": "ns=3;i=1010"    
    }, 
    { # Usina 2
        "h1": "ns=3;i=1014", "h2": "ns=3;i=1015", "h3": "ns=3;i=1016"
    },
    { # Usina 3
        "h1": "ns=3;i=1020", "h2": "ns=3;i=1021", "h3": "ns=3;i=1022"    
    }
]

def main(): 

    # Conexão com o MQTT Broker
    mqtt_client = mqtt.Client()
    mqtt_client.connect(MQTT_BROKER, MQTT_PORT, 60)

    # Conexão com o servidor OPC UA
    client = Client(SERVER_URL)

    try:
        client.connect()
        print("Conectado ao servidor OPC UA e ao MQTT Broker com sucesso!\n")

        while True:
            for usina_idx, node_ids in enumerate(node_ids_list, start=1):
                try:
                    # Leitura dos níveis dos tanques
                    h1 = client.get_node(node_ids["h1"]).get_value()
                    h2 = client.get_node(node_ids["h2"]).get_value()
                    h3 = client.get_node(node_ids["h3"]).get_value()

                    dados_usina ={
                        "h1": h1,
                        "h2": h2,
                        "h3": h3
                    }

                    output_topic = f"iot/niveis/usina{usina_idx}"
                    mqtt_client.publish(output_topic, json.dumps(dados_usina))
                    print(f"Publicando dados da Usina {usina_idx} no tópico {output_topic}: {dados_usina}")

                except Exception as e:
                    print(f"Erro ao ler Usina {usina_idx}: {e}")
            time.sleep(0.2) 
    except Exception as e:
        print(f"Erro no DataCollector: {e}")

    finally:
        client.disconnect()
        mqtt_client.disconnect()
        print("Desconectado do servidor OPC UA e do MQTT Broker.")

if __name__ == "__main__":
    main()

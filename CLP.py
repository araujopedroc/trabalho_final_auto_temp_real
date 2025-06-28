import numpy as np
from scipy.integrate import solve_ivp
from opcua import Client
import threading
import time
import random

# Parâmetros do sistema
gamma1, gamma2, gamma3 = 0.1, 0.1, 0.1
r1, R1, H1 = 1, 2, 3
r2, R2, H2 = 1, 2, 3
r3, R3, H3 = 1, 2, 3

# Condições iniciais para 3 usinas
h0_list = [np.zeros(6), np.zeros(6), np.zeros(6)]
u_list = [np.zeros(3), np.zeros(3), np.zeros(3)]
h_ref_list = [np.array([1.5, 1.6, 1.4]), np.array([1.5, 1.6, 1.4]), np.array([1.5, 1.6, 1.4])]

# Matriz de ganhos de realimentação
K = np.array([ 
    [3.5036, 2.4688, 2.2157, -0.8711, -0.3877, -0.3014],
    [-1.0348, 3.2506, 2.4688,  0.4834, -0.7849, -0.3877],
    [-0.2531, -1.0348, 3.5036,  0.0863,  0.4834, -0.8711]
])

# NodeIDs para cada usina
node_ids_list = [
    { # Usina 1
        "h1": "ns=3;i=1008", "h2": "ns=3;i=1009", "h3": "ns=3;i=1010",
        "u1": "ns=3;i=1011", "u2": "ns=3;i=1012", "u3": "ns=3;i=1013"    
    }, 
    { # Usina 2
        "h1": "ns=3;i=1014", "h2": "ns=3;i=1015", "h3": "ns=3;i=1016",
        "u1": "ns=3;i=1017", "u2": "ns=3;i=1018", "u3": "ns=3;i=1019"
    },
    { # Usina 3
        "h1": "ns=3;i=1020", "h2": "ns=3;i=1021", "h3": "ns=3;i=1022",
        "u1": "ns=3;i=1023", "u2": "ns=3;i=1024", "u3": "ns=3;i=1025"    
    }
]

def tank_dynamics_aug(t, h, usina_idx):
    u = u_list[usina_idx]
    h_ref = h_ref_list[usina_idx]
    qo1 = gamma1 * np.sqrt(h[0]) if h[0] > 0 else 0
    qo2 = gamma2 * np.sqrt(h[1]) if h[1] > 0 else 0
    qo3 = gamma3 * np.sqrt(h[2]) if h[2] > 0 else 0

    dh1dt = (u[0] - qo1 - u[1]) / (np.pi * (r1 + ((R1 - r1) / H1) * h[0]) ** 2)
    dh2dt = (u[1] - qo2 - u[2]) / (np.pi * (r2 + ((R2 - r2) / H2) * h[1]) ** 2)
    dh3dt = (u[2] - qo3) / (np.pi * (r3 + ((R3 - r3) / H3) * h[2]) ** 2)
    dh4dt = -h[0] + h_ref[0]
    dh5dt = -h[1] + h_ref[1]
    dh6dt = -h[2] + h_ref[2]

    return [dh1dt, dh2dt, dh3dt, dh4dt, dh5dt, dh6dt]

# Thread de simulação da planta
def simulate_tanks(client, stop_event, usina_idx, node_ids):
    dt = 0.2  # 200 ms
    t_current = 0

    nodes = {name: client.get_node(node_id) for name, node_id in node_ids.items()}

    while not stop_event.is_set():
        sol = solve_ivp(
            lambda t, h: tank_dynamics_aug(t, h, usina_idx),
            [t_current, t_current + dt],
            h0_list[usina_idx],
            t_eval=[t_current + dt],
            method='RK45'
        )

        h0_list[usina_idx] = sol.y[:, -1]
        t_current += dt

        nodes["h1"].set_value(h0_list[usina_idx][0])
        nodes["h2"].set_value(h0_list[usina_idx][1])
        nodes["h3"].set_value(h0_list[usina_idx][2])
      
        u_list[usina_idx][0] = nodes["u1"].get_value()
        u_list[usina_idx][1] = nodes["u2"].get_value()
        u_list[usina_idx][2] = nodes["u3"].get_value()

        time.sleep(dt)

# Thread de controle
def control_tanks(client, stop_event, usina_idx, node_ids):
    nodes = {name: client.get_node(node_id) for name, node_id in node_ids.items()}

    while not stop_event.is_set():
        h1 = nodes["h1"].get_value()
        h2 = nodes["h2"].get_value()
        h3 = nodes["h3"].get_value()

        h0_list[usina_idx][:3] = [h1, h2, h3]
        u_list[usina_idx] = -K @ h0_list[usina_idx]

        nodes["u1"].set_value(u_list[usina_idx][0])
        nodes["u2"].set_value(u_list[usina_idx][1])
        nodes["u3"].set_value(u_list[usina_idx][2])

        time.sleep(0.2)

# Thread para variar aleatoriamente os valores de referência
def randomize_references(stop_event):
    while not stop_event.is_set():
        for i in range(3):  # Para cada usina
            for j in range(3):  # Para cada tanque
                variation = random.uniform(-0.1, 0.1)
                h_ref_list[i][j] += variation
                h_ref_list[i][j] = max(0.1, min(3.0, h_ref_list[i][j]))
        
        time.sleep(5)  # Espera 5 segundos antes da próxima variação

# Configuração da conexão OPC UA
client = Client("opc.tcp://localhost:53530/OPCUA/SimulationServer")
try:
    client.connect()
    print("Conectado ao servidor OPC UA!")
except Exception as e:
    print("Erro ao conectar ao servidor OPC UA:", e)
    exit()

# Inicialização dos nós no servidor (zera valores iniciais)
try:
    for node_ids in node_ids_list:
        for node_id in node_ids.values():
            node = client.get_node(node_id)
            node.set_value(0.0)
except Exception as e:
    print(f"Erro ao inicializar os nós: {e}")

# Criação e início das threads 
stop_event = threading.Event()
threads = []

for i in range(3):
    sim_thread = threading.Thread(target=simulate_tanks, args=(client, stop_event, i, node_ids_list[i]))
    control_thread = threading.Thread(target=control_tanks, args=(client, stop_event, i, node_ids_list[i]))
    threads.extend([sim_thread, control_thread])
    sim_thread.start()
    control_thread.start()

# Thread para variar os valores de referência
ref_thread = threading.Thread(target=randomize_references, args=(stop_event,))
threads.append(ref_thread)
ref_thread.start()

# Loop principal (executa até interrupção por teclado)
print("Simulação em execução. Pressione Ctrl+C para parar.")
try:
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    print("\nInterrompendo a simulação...")
    stop_event.set()

# Finalização 
for thread in threads:
    thread.join()

client.disconnect()
print("Simulação finalizada com sucesso!")
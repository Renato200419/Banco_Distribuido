import socket
import threading
import os
import time 
from datetime import datetime
import argparse 

# --- Configuración del Nodo 
DEFAULT_ID_NODO = 3
DEFAULT_IP_SERVIDOR_CENTRAL = '192.168.18.29'
DEFAULT_PUERTO_SERVIDOR_CENTRAL = 9000 # Puerto donde el ServidorCentral Java escucha
DATA_DIR_RELATIVE = os.path.join("..", "data") # Ruta relativa al script

# --- Variables Globales (se inicializarán en main o funciones) ---
ID_NODO = DEFAULT_ID_NODO
PUERTO_NODO = 9100 + ID_NODO # Se calculará o tomará de args
IP_SERVIDOR_CENTRAL = DEFAULT_IP_SERVIDOR_CENTRAL
PUERTO_SERVIDOR_CENTRAL = DEFAULT_PUERTO_SERVIDOR_CENTRAL
DATA_DIR = "" # Se resolverá a una ruta absoluta
LOG_FILE_PATH = ""

clientes_data = {}
cuentas_data = {} # {id_cuenta: {"id_cliente": ..., "saldo": ..., "tipo_cuenta": "...", "lock": threading.Lock()}}
transacciones_data = [] # Lista de diccionarios de transacciones
transacciones_file_lock = threading.Lock()
particiones_nodo = set()

def log_message(message):
    global LOG_FILE_PATH
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_line = f"[{timestamp}] [NodoPy {ID_NODO} P:{PUERTO_NODO}] {message}" # Añadido puerto para claridad
    print(log_line)
    if LOG_FILE_PATH: # Solo escribir si LOG_FILE_PATH está definido
        try:
            # Asegurar que el directorio de logs exista
            os.makedirs(os.path.dirname(LOG_FILE_PATH), exist_ok=True)
            with open(LOG_FILE_PATH, "a", encoding='utf-8') as f:
                f.write(log_line + "\n")
        except Exception as e:
            print(f"Error escribiendo en log '{LOG_FILE_PATH}': {e}")

def configurar_particiones_nodo_default(id_nodo_actual):
    global particiones_nodo
    particiones_nodo.clear() # Limpiar por si se llama múltiples veces
    # Lógica similar a tu NodoTrabajador.java o .ts
    if id_nodo_actual == 1:
        particiones_nodo.update(["parte1", "parte2", "parte3"])
    elif id_nodo_actual == 2: # Este nodo TypeScript
        particiones_nodo.update(["parte1", "parte2", "parte4"])
    elif id_nodo_actual == 3: # Este nodo Python
        particiones_nodo.update(["parte2", "parte3", "parte4"])
    elif id_nodo_actual == 4: # Nodo JavaScript
        particiones_nodo.update(["parte1", "parte3", "parte4"])
    else:
        particiones_nodo.update([f"default_part_for_nodo{id_nodo_actual}.1", f"default_part_for_nodo{id_nodo_actual}.2"])
    log_message(f"Particiones por defecto configuradas: {particiones_nodo}")

def crear_directorios_si_no_existen():
    global DATA_DIR
    try:
        os.makedirs(os.path.join(DATA_DIR, "clientes"), exist_ok=True)
        os.makedirs(os.path.join(DATA_DIR, "cuentas"), exist_ok=True)
        os.makedirs(os.path.join(DATA_DIR, "transacciones"), exist_ok=True)
    except Exception as e:
        log_message(f"Error creando directorios de datos: {e}")

def cargar_datos_iniciales():
    global clientes_data, cuentas_data, transacciones_data, DATA_DIR
    
    # Asegurar que DATA_DIR es una ruta absoluta basada en la ubicación del script
    script_dir = os.path.dirname(os.path.abspath(__file__))
    DATA_DIR = os.path.abspath(os.path.join(script_dir, DATA_DIR_RELATIVE))
    log_message(f"Directorio de datos resolvedo a: {DATA_DIR}")

    crear_directorios_si_no_existen()

    # Cargar Clientes
    clientes_file = os.path.join(DATA_DIR, "clientes", "clientes.txt")
    if not os.path.exists(clientes_file):
        log_message("Archivo de clientes no encontrado. Creando archivo de prueba...")
        with open(clientes_file, "w", encoding='utf-8') as f:
            f.write("1|Juan Pérez|juan@email.com|987654321\n")
            # ... (más datos de ejemplo si quieres)
    
    clientes_data.clear()
    with open(clientes_file, "r", encoding='utf-8') as f:
        for linea in f:
            linea = linea.strip()
            if not linea: continue
            partes = linea.split('|')
            if len(partes) >= 4:
                id_cliente = int(partes[0])
                clientes_data[id_cliente] = {"nombre": partes[1], "email": partes[2], "telefono": partes[3]}
    log_message(f"Clientes cargados: {len(clientes_data)}")

    # Cargar Cuentas
    cuentas_file = os.path.join(DATA_DIR, "cuentas", "cuentas.txt")
    if not os.path.exists(cuentas_file):
        log_message("Archivo de cuentas no encontrado. Creando archivo de prueba...")
        with open(cuentas_file, "w", encoding='utf-8') as f:
            f.write("101|1|1500.00|Ahorros\n")
            f.write("102|2|3200.50|Corriente\n")
            f.write("201|1|500.00|Ahorros\n") 
            f.write("202|2|800.00|Corriente\n")
    
    cuentas_data.clear()
    with open(cuentas_file, "r", encoding='utf-8') as f:
        for linea in f:
            linea = linea.strip()
            if not linea: continue
            partes = linea.split('|')
            if len(partes) >= 4:
                id_cuenta = int(partes[0])
                # Aquí iría la lógica para cargar solo datos de particiones asignadas
                # si DATA_DIR apuntara a archivos de partición específicos.
                # Por ahora, carga todo del archivo global.
                cuentas_data[id_cuenta] = {
                    "id_cliente": int(partes[1]),
                    "saldo": float(partes[2]),
                    "tipo_cuenta": partes[3],
                    "lock": threading.Lock()
                }
    log_message(f"Cuentas cargadas: {len(cuentas_data)}")

    # Cargar Transacciones
    transacciones_file = os.path.join(DATA_DIR, "transacciones", "transacciones.txt")
    if not os.path.exists(transacciones_file):
        log_message("Archivo de transacciones no encontrado. Creando archivo vacío...")
        open(transacciones_file, "w", encoding='utf-8').close()
    
    transacciones_data.clear()
    with open(transacciones_file, "r", encoding='utf-8') as f:
        for linea in f:
            linea = linea.strip();
            if not linea: continue
            partes = linea.split('|')
            if len(partes) >= 6:
                transacciones_data.append({
                    "id_transacc": int(partes[0]), "id_orig": int(partes[1]),
                    "id_dest": int(partes[2]), "monto": float(partes[3]),
                    "fecha_hora": partes[4], "estado": partes[5]
                })
    log_message(f"Transacciones previas cargadas: {len(transacciones_data)}")


def guardar_cuentas_a_archivo():
    global DATA_DIR
    cuentas_file = os.path.join(DATA_DIR, "cuentas", "cuentas.txt")
    try:
        with open(cuentas_file, "w", encoding='utf-8') as f:
            for id_cuenta, data in cuentas_data.items():
                f.write(f"{id_cuenta}|{data['id_cliente']}|{data['saldo']:.2f}|{data['tipo_cuenta']}\n")
        
    except Exception as e:
        log_message(f"Error guardando cuentas: {e}")

def guardar_transaccion_a_archivo(transaccion):
    global DATA_DIR
    transacciones_file = os.path.join(DATA_DIR, "transacciones", "transacciones.txt")
    with transacciones_file_lock:
        try:
            with open(transacciones_file, "a", encoding='utf-8') as f:
                f.write(f"{transaccion['id_transacc']}|{transaccion['id_orig']}|"
                        f"{transaccion['id_dest']}|{transaccion['monto']:.2f}|"
                        f"{transaccion['fecha_hora']}|{transaccion['estado']}\n")
            
        except Exception as e:
            log_message(f"Error guardando transacción: {e}")

def procesar_consulta_saldo(params):
    if len(params) < 1: return "ERROR|Faltan parámetros para consultar saldo"
    try:
        id_cuenta = int(params[0])
        if id_cuenta not in cuentas_data: return f"ERROR|Cuenta no encontrada: {id_cuenta}"
        
        cuenta_lock = cuentas_data[id_cuenta].get("lock")
        if not cuenta_lock: return f"ERROR|Lock no encontrado para cuenta {id_cuenta}"

        if cuenta_lock.acquire(timeout=0.5): # Timeout corto
            try:
                saldo = cuentas_data[id_cuenta]["saldo"]
                return f"OK|{saldo:.2f}"
            finally:
                cuenta_lock.release()
        else:
            return f"ERROR|Timeout adquiriendo lock para cuenta {id_cuenta}"
            
    except ValueError: return "ERROR|ID de cuenta inválido"
    except Exception as e:
        log_message(f"Error en consultarSaldo: {e}"); return f"ERROR|Interno: {e}"

def procesar_transferir_fondos(params):
    if len(params) < 3: return "ERROR|Faltan parámetros para transferencia"
    try:
        cuenta_origen_id = int(params[0])
        cuenta_destino_id = int(params[1])
        monto = float(params[2])

        if monto <= 0: return "ERROR|El monto debe ser positivo"
        if cuenta_origen_id not in cuentas_data: return f"ERROR|Cuenta origen no encontrada: {cuenta_origen_id}"
        if cuenta_destino_id not in cuentas_data: return f"ERROR|Cuenta destino no encontrada: {cuenta_destino_id}"
        if cuenta_origen_id == cuenta_destino_id: return "ERROR|Cuenta origen y destino no pueden ser la misma"

        # Adquirir locks en orden para evitar deadlocks
        lock_ids = sorted([cuenta_origen_id, cuenta_destino_id])
        lock1 = cuentas_data[lock_ids[0]]["lock"]
        lock2 = cuentas_data[lock_ids[1]]["lock"]
        
        adquirido1 = lock1.acquire(timeout=0.5)
        if not adquirido1: return f"ERROR|Timeout lock cuenta {lock_ids[0]}"
        
        adquirido2 = lock2.acquire(timeout=0.5)
        if not adquirido2:
            lock1.release()
            return f"ERROR|Timeout lock cuenta {lock_ids[1]}"

        try:
            if cuentas_data[cuenta_origen_id]["saldo"] < monto:
                return "ERROR|Saldo insuficiente"

            cuentas_data[cuenta_origen_id]["saldo"] -= monto
            cuentas_data[cuenta_destino_id]["saldo"] += monto

            # Transacción
            # Usa el lock global de transacciones_data si vas a modificar la lista en memoria
            # y luego el lock de archivo para escribir.
            with transacciones_file_lock: # Protege la generación de ID si se basa en len(transacciones_data)
                 id_transaccion = len(transacciones_data) + sum(1 for t in transacciones_data if 'id_transacc' in t) + 1 # Un ID más robusto si se borran
            
            fecha_hora_actual = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3] # Incluye milisegundos
            nueva_transaccion = {
                "id_transacc": id_transaccion, "id_orig": cuenta_origen_id,
                "id_dest": cuenta_destino_id, "monto": monto,
                "fecha_hora": fecha_hora_actual, "estado": "Confirmada"
            }
            # Añadir a la lista en memoria (si la usas para algo más que cargarla)
            # transacciones_data.append(nueva_transaccion) # Podrías necesitar un lock si múltiples hilos modifican esta lista

            guardar_cuentas_a_archivo() # Guarda todas las cuentas
            guardar_transaccion_a_archivo(nueva_transaccion) # Añade la nueva transacción

            return "OK|Transferencia completada"
        finally:
            lock2.release()
            lock1.release()

    except ValueError: return "ERROR|Parámetros inválidos"
    except Exception as e:
        log_message(f"Error en transferirFondos: {e}"); return f"ERROR|Interno: {e}"

def manejar_conexion_cliente(client_socket, client_address):
    log_message(f"Conexión aceptada de {client_address}")
    try:
        buffer = ""
        while True:
            data = client_socket.recv(1024)
            if not data:
                log_message(f"Cliente {client_address} desconectado (no data).")
                break
            
            buffer += data.decode('utf-8')
            
            while '\n' in buffer:
                mensaje_completo, buffer = buffer.split('\n', 1)
                mensaje_completo = mensaje_completo.strip()
                if not mensaje_completo: continue

                log_message(f"Recibido de ServidorCentral: {mensaje_completo}")
                partes = mensaje_completo.split('|')

                if len(partes) < 3 or partes[0] != "TASK":
                    respuesta = "ERROR|Formato de solicitud inválido\n"
                else:
                    id_tarea = partes[1]
                    operacion = partes[2]
                    params = partes[3:]
                    resultado_op = ""

                    if operacion == "CONSULTAR_SALDO":
                        resultado_op = procesar_consulta_saldo(params)
                    elif operacion == "TRANSFERIR_FONDOS":
                        resultado_op = procesar_transferir_fondos(params)
                    else:
                        resultado_op = f"ERROR|Operación no soportada: {operacion}"
                    respuesta = f"RESPONSE|{id_tarea}|{resultado_op}\n"
                
                log_message(f"Enviando a ServidorCentral: {respuesta.strip()}")
                client_socket.sendall(respuesta.encode('utf-8'))

    except ConnectionResetError: log_message(f"Conexión reseteada por {client_address}")
    except socket.timeout: log_message(f"Socket timeout para {client_address}")
    except Exception as e: log_message(f"Error manejando {client_address}: {e}")
    finally:
        client_socket.close(); log_message(f"Conexión con {client_address} cerrada.")

def iniciar_servidor_nodo():
    global PUERTO_NODO
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    
    try:
        server_socket.bind(('', PUERTO_NODO))
        server_socket.listen(10) # Aumentado un poco el backlog
        log_message(f"Nodo trabajador Python escuchando en el puerto {PUERTO_NODO}")

        while True:
            client_socket, client_address = server_socket.accept()
            client_socket.settimeout(60) # Timeout para operaciones de socket
            thread_cliente = threading.Thread(target=manejar_conexion_cliente, args=(client_socket, client_address), daemon=True)
            thread_cliente.start()
    except OSError as e:
        log_message(f"Error al iniciar el servidor del nodo (OSError): {e}. ¿Puerto {PUERTO_NODO} en uso?")
    except Exception as e:
        log_message(f"Error crítico en el servidor del nodo: {e}")
    finally:
        server_socket.close(); log_message("Servidor del nodo detenido.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Nodo Trabajador Python.")
    parser.add_argument("id_nodo", type=int, nargs='?', default=DEFAULT_ID_NODO, help=f"ID del nodo (def: {DEFAULT_ID_NODO}).")
    parser.add_argument("ip_servidor_central", type=str, nargs='?', default=DEFAULT_IP_SERVIDOR_CENTRAL, help=f"IP del Servidor Central (def: {DEFAULT_IP_SERVIDOR_CENTRAL}).")
    parser.add_argument("--puerto_nodo", type=int, default=None, help="Puerto del nodo (def: 9100 + id_nodo).")
    parser.add_argument("--puerto_servidor_central", type=int, default=DEFAULT_PUERTO_SERVIDOR_CENTRAL, help=f"Puerto del Servidor Central (def: {DEFAULT_PUERTO_SERVIDOR_CENTRAL}).")
    parser.add_argument("--particiones", type=str, nargs='*', help="Lista de particiones que maneja este nodo.")
    parser.add_argument("--data_dir_relative", type=str, default=DATA_DIR_RELATIVE, help=f"Ruta relativa al dir de datos (def: {DATA_DIR_RELATIVE}).")

    args = parser.parse_args()

    ID_NODO = args.id_nodo
    IP_SERVIDOR_CENTRAL = args.ip_servidor_central
    PUERTO_NODO = args.puerto_nodo if args.puerto_nodo is not None else (9100 + ID_NODO)
    PUERTO_SERVIDOR_CENTRAL = args.puerto_servidor_central # Usar el argumento
    DATA_DIR_RELATIVE = args.data_dir_relative # Usar el argumento

    # Construir LOG_FILE_PATH después de que ID_NODO esté definido
    script_dir_for_log = os.path.dirname(os.path.abspath(__file__))
    LOG_FILE_PATH = os.path.abspath(os.path.join(script_dir_for_log, "..", "logs", f"nodo{ID_NODO}.log"))


    log_message(f"--- Iniciando Nodo Trabajador Python ID: {ID_NODO} ---")
    log_message(f"Puerto del Nodo         : {PUERTO_NODO}")
    log_message(f"Conectando a Serv. Cent.: {IP_SERVIDOR_CENTRAL}:{PUERTO_SERVIDOR_CENTRAL}")
    log_message(f"Ruta relativa de datos  : {DATA_DIR_RELATIVE}")


    if args.particiones:
        particiones_nodo.update(args.particiones)
        log_message(f"Particiones asignadas desde argumentos: {particiones_nodo}")
    else:
        configurar_particiones_nodo_default(ID_NODO)

    cargar_datos_iniciales() # DATA_DIR se resuelve dentro de esta función
    iniciar_servidor_nodo()

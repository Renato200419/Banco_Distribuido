import java.io.*;
import java.net.*;
import java.util.*;
import java.text.SimpleDateFormat;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantLock;
import java.util.Date;
import java.nio.file.*;

/**
 * NodoTrabajador - Implementación Java de un nodo trabajador
 * 
 * CORREGIDO PARA FUNCIONAR CON EL SERVIDOR CENTRAL
 * - Carga datos desde particiones correctas
 * - Maneja miles de cuentas
 * - Compatible con IDs 101-5100
 */
public class NodoTrabajador {
    // Configuración del nodo
    private static int ID_NODO = 1;
    private static int PUERTO = 9101;
    private static String IP_SERVIDOR_CENTRAL = "192.168.18.31"; // IP del servidor central
    private static int PUERTO_SERVIDOR_CENTRAL = 9000;
    
    // Directorio de datos (CORREGIDO PARA TU ESTRUCTURA)
    private static final String DATA_DIR = "data"; // Sin ../ porque está en el mismo directorio
    
    // Conjunto de particiones que gestiona este nodo
    private static Set<String> particiones = new HashSet<>();
    
    // Para registro de actividad
    private static final SimpleDateFormat formatoFecha = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private static String LOG_FILE = "../logs/nodo1.log";
    
    // Estructuras de datos para clientes y cuentas
    private static Map<Integer, Map<String, String>> clientes = new HashMap<>();
    private static Map<Integer, Map<String, Object>> cuentas = new HashMap<>();
    private static List<Map<String, Object>> transacciones = new ArrayList<>();
    
    // Locks para operaciones concurrentes
    private static Map<Integer, ReentrantLock> cuentaLocks = new HashMap<>();
    private static final ReentrantLock transaccionLock = new ReentrantLock();
    
    // Pool de hilos para procesar solicitudes
    private static ExecutorService threadPool;
    
    /**
     * Método principal
     */
    public static void main(String[] args) {
        // Procesar argumentos si se proporcionan
        if (args.length >= 1) {
            ID_NODO = Integer.parseInt(args[0]);
            PUERTO = 9100 + ID_NODO;
            LOG_FILE = "logs/nodo" + ID_NODO + ".log"; // Sin ../
        }
        
        if (args.length >= 2) {
            IP_SERVIDOR_CENTRAL = args[1];
        }
        
        // Inicializar el nodo
        try {
            inicializar();
            
            // Iniciar el servidor
            iniciarServidor();
        } catch (Exception e) {
            log("Error inicializando el nodo: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    /**
     * Inicializa el nodo trabajador
     */
    private static void inicializar() throws IOException {
        log("=== NODO TRABAJADOR JAVA ===");
        log("ID Nodo: " + ID_NODO + ", Puerto: " + PUERTO);
        log("Servidor Central: " + IP_SERVIDOR_CENTRAL + ":" + PUERTO_SERVIDOR_CENTRAL);
        
        // Crear directorios necesarios
        crearDirectorios();
        
        // Configurar particiones según el ID del nodo
        configurarParticiones();
        
        // Cargar datos
        cargarDatos();
        
        // Crear pool de hilos
        int numThreads = Runtime.getRuntime().availableProcessors() * 2;
        threadPool = Executors.newFixedThreadPool(numThreads);
        log("Pool de hilos creado con " + numThreads + " hilos");
        
        log("Nodo trabajador inicializado correctamente");
    }
    
    /**
     * Crea directorios necesarios
     */
    private static void crearDirectorios() {
        try {
            // Crear directorio de logs
            Files.createDirectories(Paths.get("logs"));
            
            // Crear directorios de datos si no existen
            Files.createDirectories(Paths.get(DATA_DIR + "/clientes"));
            Files.createDirectories(Paths.get(DATA_DIR + "/transacciones"));
            
            // Crear directorios de particiones
            for (int i = 1; i <= 4; i++) {
                Files.createDirectories(Paths.get(DATA_DIR + "/parte" + i));
            }
            
            log("Directorios creados correctamente");
        } catch (IOException e) {
            log("Error creando directorios: " + e.getMessage());
        }
    }
    
    /**
     * Configura las particiones que gestionará este nodo
     */
    private static void configurarParticiones() {
        // Configuración según ID del nodo (compatible con ServidorCentral)
        switch (ID_NODO) {
            case 1:
                particiones.addAll(Arrays.asList("parte1", "parte2", "parte3"));
                break;
                
            case 2:
                particiones.addAll(Arrays.asList("parte1", "parte3", "parte4"));
                break;
                
            case 3:
                particiones.addAll(Arrays.asList("parte2", "parte3", "parte4"));
                break;
                
            case 4:
                particiones.addAll(Arrays.asList("parte1", "parte2", "parte4"));
                break;
                
            default:
                // Configuración por defecto
                particiones.addAll(Arrays.asList("parte1", "parte2"));
                break;
        }
        
        log("Particiones configuradas: " + particiones);
    }
    
    /**
     * Carga los datos de clientes y cuentas
     */
    private static void cargarDatos() throws IOException {
        log("Iniciando carga de datos...");
        
        // Cargar clientes
        cargarClientes();
        
        // Cargar cuentas desde particiones
        cargarCuentasDesdeParticiones();
        
        // Cargar transacciones
        cargarTransacciones();
        
        log("Datos cargados correctamente:");
        log("  - Clientes: " + clientes.size());
        log("  - Cuentas: " + cuentas.size());
        log("  - Transacciones: " + transacciones.size());
    }
    
    /**
     * Carga los datos de clientes
     */
    private static void cargarClientes() throws IOException {
        File clientesFile = new File(DATA_DIR + "/clientes/clientes.txt");
        
        if (!clientesFile.exists()) {
            log("Archivo de clientes no encontrado, esperando que el servidor lo cree...");
            return;
        }
        
        try (BufferedReader reader = new BufferedReader(new FileReader(clientesFile))) {
            String linea;
            while ((linea = reader.readLine()) != null) {
                if (linea.trim().isEmpty()) continue;
                
                String[] partes = linea.split("\\|");
                if (partes.length >= 4) {
                    int idCliente = Integer.parseInt(partes[0]);
                    Map<String, String> cliente = new HashMap<>();
                    cliente.put("nombre", partes[1]);
                    cliente.put("email", partes[2]);
                    cliente.put("telefono", partes[3]);
                    clientes.put(idCliente, cliente);
                }
            }
        }
        
        log("Clientes cargados: " + clientes.size());
    }
    
    /**
     * Carga cuentas desde las particiones configuradas
     */
    private static void cargarCuentasDesdeParticiones() throws IOException {
        for (String particion : particiones) {
            File particionFile = new File(DATA_DIR + "/" + particion + "/cuentas_" + particion + ".txt");
            
            if (!particionFile.exists()) {
                log("Archivo de partición no encontrado: " + particionFile.getPath());
                continue;
            }
            
            int cuentasCargadas = 0;
            try (BufferedReader reader = new BufferedReader(new FileReader(particionFile))) {
                String linea;
                while ((linea = reader.readLine()) != null) {
                    if (linea.trim().isEmpty()) continue;
                    
                    String[] partes = linea.split("\\|");
                    if (partes.length >= 4) {
                        int idCuenta = Integer.parseInt(partes[0]);
                        Map<String, Object> cuenta = new HashMap<>();
                        cuenta.put("id_cliente", Integer.parseInt(partes[1]));
                        cuenta.put("saldo", Double.parseDouble(partes[2]));
                        cuenta.put("tipo_cuenta", partes[3]);
                        cuentas.put(idCuenta, cuenta);
                        
                        // Crear lock para esta cuenta
                        cuentaLocks.put(idCuenta, new ReentrantLock());
                        cuentasCargadas++;
                    }
                }
            }
            
            log("Partición " + particion + ": " + cuentasCargadas + " cuentas cargadas");
        }
    }
    
    /**
     * Carga las transacciones
     */
    private static void cargarTransacciones() throws IOException {
        File transaccionesFile = new File(DATA_DIR + "/transacciones/transacciones.txt");
        
        if (!transaccionesFile.exists()) {
            log("Archivo de transacciones no encontrado, creando archivo vacío...");
            transaccionesFile.createNewFile();
            return;
        }
        
        try (BufferedReader reader = new BufferedReader(new FileReader(transaccionesFile))) {
            String linea;
            while ((linea = reader.readLine()) != null) {
                if (linea.trim().isEmpty()) continue;
                
                String[] partes = linea.split("\\|");
                if (partes.length >= 6) {
                    Map<String, Object> transaccion = new HashMap<>();
                    transaccion.put("id_transacc", Integer.parseInt(partes[0]));
                    transaccion.put("id_orig", Integer.parseInt(partes[1]));
                    transaccion.put("id_dest", Integer.parseInt(partes[2]));
                    transaccion.put("monto", Double.parseDouble(partes[3]));
                    transaccion.put("fecha_hora", partes[4]);
                    transaccion.put("estado", partes[5]);
                    transacciones.add(transaccion);
                }
            }
        }
    }
    
    /**
     * Inicia el servidor para escuchar solicitudes
     */
    private static void iniciarServidor() {
        log("Iniciando servidor en puerto " + PUERTO);
        
        try (ServerSocket serverSocket = new ServerSocket(PUERTO)) {
            log("Servidor listo para recibir conexiones");
            
            while (true) {
                try {
                    Socket clienteSocket = serverSocket.accept();
                    threadPool.submit(() -> manejarSolicitud(clienteSocket));
                } catch (IOException e) {
                    log("Error aceptando conexión: " + e.getMessage());
                }
            }
        } catch (IOException e) {
            log("Error en el servidor: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    /**
     * Maneja una solicitud entrante
     */
    private static void manejarSolicitud(Socket socket) {
        try (
            BufferedReader entrada = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            PrintWriter salida = new PrintWriter(socket.getOutputStream(), true)
        ) {
            // Leer la solicitud
            String solicitud = entrada.readLine();
            
            if (solicitud == null) {
                // Health check o conexión cerrada
                return;
            }
            
            log("Solicitud recibida: " + solicitud);
            
            // Parsear la solicitud: TASK|idTarea|operacion|param1|param2|...
            String[] partes = solicitud.split("\\|");
            
            if (partes.length < 3 || !partes[0].equals("TASK")) {
                salida.println("ERROR|Formato de solicitud inválido");
                return;
            }
            
            int idTarea = Integer.parseInt(partes[1]);
            String operacion = partes[2];
            String[] parametros = Arrays.copyOfRange(partes, 3, partes.length);
            
            // Procesar según la operación
            String resultado;
            
            switch (operacion) {
                case "CONSULTAR_SALDO":
                    resultado = consultarSaldo(parametros);
                    break;
                    
                case "TRANSFERIR_FONDOS":
                    resultado = transferirFondos(parametros);
                    break;
                    
                default:
                    resultado = "ERROR|Operación no soportada: " + operacion;
                    break;
            }
            
            // Enviar respuesta: RESPONSE|idTarea|resultado
            String respuesta = "RESPONSE|" + idTarea + "|" + resultado;
            salida.println(respuesta);
            log("Respuesta enviada para tarea " + idTarea + ": " + resultado);
            
        } catch (Exception e) {
            log("Error procesando solicitud: " + e.getMessage());
        }
    }
    
    /**
     * Consulta el saldo de una cuenta
     */
    private static String consultarSaldo(String[] parametros) {
        if (parametros.length < 1) {
            return "ERROR|Faltan parámetros para consultar saldo";
        }
        
        try {
            int idCuenta = Integer.parseInt(parametros[0]);
            
            // Verificar si tenemos la cuenta
            if (!cuentas.containsKey(idCuenta)) {
                return "ERROR|Cuenta no encontrada: " + idCuenta;
            }
            
            // Obtener el saldo con lock
            ReentrantLock lock = cuentaLocks.get(idCuenta);
            if (lock == null) {
                return "ERROR|Lock no disponible para cuenta: " + idCuenta;
            }
            
            lock.lock();
            try {
                double saldo = (double) cuentas.get(idCuenta).get("saldo");
                return "OK|" + String.format("%.2f", saldo);
            } finally {
                lock.unlock();
            }
            
        } catch (NumberFormatException e) {
            return "ERROR|ID de cuenta inválido";
        } catch (Exception e) {
            log("Error consultando saldo: " + e.getMessage());
            return "ERROR|" + e.getMessage();
        }
    }
    
    /**
     * Realiza una transferencia entre cuentas
     */
    private static String transferirFondos(String[] parametros) {
        if (parametros.length < 3) {
            return "ERROR|Faltan parámetros para transferencia";
        }
        
        try {
            int cuentaOrigen = Integer.parseInt(parametros[0]);
            int cuentaDestino = Integer.parseInt(parametros[1]);
            double monto = Double.parseDouble(parametros[2]);
            
            // Validaciones básicas
            if (monto <= 0) {
                return "ERROR|El monto debe ser positivo";
            }
            
            if (!cuentas.containsKey(cuentaOrigen)) {
                return "ERROR|Cuenta origen no encontrada: " + cuentaOrigen;
            }
            
            if (!cuentas.containsKey(cuentaDestino)) {
                return "ERROR|Cuenta destino no encontrada: " + cuentaDestino;
            }
            
            // Adquirir locks en orden para evitar deadlocks
            ReentrantLock lockOrigen = cuentaLocks.get(cuentaOrigen);
            ReentrantLock lockDestino = cuentaLocks.get(cuentaDestino);
            
            if (lockOrigen == null || lockDestino == null) {
                return "ERROR|Locks no disponibles para las cuentas";
            }
            
            ReentrantLock firstLock = cuentaOrigen < cuentaDestino ? lockOrigen : lockDestino;
            ReentrantLock secondLock = cuentaOrigen < cuentaDestino ? lockDestino : lockOrigen;
            
            firstLock.lock();
            try {
                secondLock.lock();
                try {
                    // Verificar saldo suficiente
                    double saldoOrigen = (double) cuentas.get(cuentaOrigen).get("saldo");
                    
                    if (saldoOrigen < monto) {
                        return "ERROR|Saldo insuficiente. Disponible: " + String.format("%.2f", saldoOrigen);
                    }
                    
                    // Realizar la transferencia
                    cuentas.get(cuentaOrigen).put("saldo", saldoOrigen - monto);
                    
                    double saldoDestino = (double) cuentas.get(cuentaDestino).get("saldo");
                    cuentas.get(cuentaDestino).put("saldo", saldoDestino + monto);
                    
                    // Registrar la transacción
                    registrarTransaccion(cuentaOrigen, cuentaDestino, monto);
                    
                    return "OK|Transferencia completada";
                    
                } finally {
                    secondLock.unlock();
                }
            } finally {
                firstLock.unlock();
            }
            
        } catch (NumberFormatException e) {
            return "ERROR|Parámetros inválidos";
        } catch (Exception e) {
            log("Error en transferencia: " + e.getMessage());
            return "ERROR|" + e.getMessage();
        }
    }
    
    /**
     * Registra una transacción
     */
    private static void registrarTransaccion(int cuentaOrigen, int cuentaDestino, double monto) {
        transaccionLock.lock();
        try {
            int idTransaccion = transacciones.size() + 1;
            
            Map<String, Object> transaccion = new HashMap<>();
            transaccion.put("id_transacc", idTransaccion);
            transaccion.put("id_orig", cuentaOrigen);
            transaccion.put("id_dest", cuentaDestino);
            transaccion.put("monto", monto);
            transaccion.put("fecha_hora", formatoFecha.format(new Date()));
            transaccion.put("estado", "Confirmada");
            
            transacciones.add(transaccion);
            
            log("Transacción registrada: " + cuentaOrigen + " -> " + cuentaDestino + " ($" + monto + ")");
            
        } finally {
            transaccionLock.unlock();
        }
    }
    
    /**
     * Registra un mensaje de log
     */
    private static void log(String mensaje) {
        String timestamp = formatoFecha.format(new Date());
        String logLine = "[" + timestamp + "] " + mensaje;
        
        // Imprimir en consola
        System.out.println(logLine);
        
        // Escribir en archivo de log
        try {
            File logDir = new File("logs");
            if (!logDir.exists()) {
                logDir.mkdirs();
            }
            
            try (FileWriter fw = new FileWriter(LOG_FILE, true);
                 BufferedWriter bw = new BufferedWriter(fw);
                 PrintWriter out = new PrintWriter(bw)) {
                out.println(logLine);
            }
        } catch (IOException e) {
            System.err.println("Error escribiendo en log: " + e.getMessage());
        }
    }
}

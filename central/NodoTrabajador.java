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
 * Este nodo procesará solicitudes del Servidor Central para consultar saldos 
 * y realizar transferencias entre cuentas. Gestiona particiones de datos
 * y soporta replicación.
 */
public class NodoTrabajador {
    // Configuración del nodo
    private static int ID_NODO = 1;
    private static int PUERTO = 9101;
    private static String IP_SERVIDOR_CENTRAL = "192.168.18.29"; // Actualizar con la IP real
    private static int PUERTO_SERVIDOR_CENTRAL = 9000;
    
    // Directorio de datos
    private static final String DATA_DIR = "../data";
    
    // Conjunto de particiones que gestiona este nodo
    private static Set<String> particiones = new HashSet<>();
    
    // Para registro de actividad
    private static final SimpleDateFormat formatoFecha = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private static final String LOG_FILE = "../logs/nodo" + ID_NODO + ".log";
    
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
        log("Iniciando Nodo Trabajador " + ID_NODO + " en puerto " + PUERTO);
        
        // Configurar particiones según el ID del nodo
        configurarParticiones();
        
        // Cargar datos
        cargarDatos();
        
        // Crear pool de hilos
        int numThreads = Runtime.getRuntime().availableProcessors();
        threadPool = Executors.newFixedThreadPool(numThreads);
        log("Pool de hilos creado con " + numThreads + " hilos");
        
        // Registrar con el servidor central (esto sería en un caso real)
        // Por ahora asumimos que el servidor central ya conoce los nodos
    }
    
    /**
     * Configura las particiones que gestionará este nodo
     */
    private static void configurarParticiones() {
        // Esto dependerá del ID del nodo y debe coincidir con la configuración
        // del servidor central
        switch (ID_NODO) {
            case 1:
                particiones.addAll(Arrays.asList("parte1.1", "parte2.1", "parte2.2", "parte2.3"));
                break;
                
            case 2:
                particiones.addAll(Arrays.asList("parte1.1", "parte1.2", "parte2.2", "parte2.3", "parte2.4"));
                break;
                
            case 3:
                particiones.addAll(Arrays.asList("parte1.1", "parte1.2", "parte1.3", "parte2.3", "parte2.4"));
                break;
                
            default:
                // Configuración genérica para otros nodos
                particiones.addAll(Arrays.asList("parte1.1", "parte2.1"));
                break;
        }
        
        log("Particiones configuradas: " + particiones);
    }
    
    /**
     * Carga los datos de clientes y cuentas
     */
    private static void cargarDatos() throws IOException {
        // Cargar clientes
        log("Cargando datos de clientes...");
        cargarClientes();
        
        // Cargar cuentas
        log("Cargando datos de cuentas...");
        cargarCuentas();
        
        // Cargar transacciones
        log("Cargando datos de transacciones...");
        cargarTransacciones();
        
        log("Datos cargados correctamente. Clientes: " + clientes.size() + 
            ", Cuentas: " + cuentas.size() + ", Transacciones: " + transacciones.size());
    }
    
    /**
     * Carga los datos de clientes desde archivos
     */
    private static void cargarClientes() throws IOException {
        File clientesDir = new File(DATA_DIR + "/clientes");
        if (!clientesDir.exists()) {
            log("Directorio de clientes no encontrado. Creando directorio...");
            clientesDir.mkdirs();
        }
        
        File clientesFile = new File(DATA_DIR + "/clientes/clientes.txt");
        if (!clientesFile.exists()) {
            log("Archivo de clientes no encontrado. Creando archivo de prueba...");
            try (PrintWriter writer = new PrintWriter(clientesFile)) {
                writer.println("1|Juan Pérez|juan@email.com|987654321");
                writer.println("2|María López|maria@email.com|998877665");
                writer.println("3|Carlos Rodríguez|carlos@email.com|912345678");
            }
        }
        
        try (BufferedReader reader = new BufferedReader(new FileReader(clientesFile))) {
            String linea;
            while ((linea = reader.readLine()) != null) {
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
    }
    
    /**
     * Carga los datos de cuentas desde archivos
     */
    private static void cargarCuentas() throws IOException {
        File cuentasDir = new File(DATA_DIR + "/cuentas");
        if (!cuentasDir.exists()) {
            log("Directorio de cuentas no encontrado. Creando directorio...");
            cuentasDir.mkdirs();
        }
        
        File cuentasFile = new File(DATA_DIR + "/cuentas/cuentas.txt");
        if (!cuentasFile.exists()) {
            log("Archivo de cuentas no encontrado. Creando archivo de prueba...");
            try (PrintWriter writer = new PrintWriter(cuentasFile)) {
                writer.println("101|1|1500.00|Ahorros");
                writer.println("102|2|3200.50|Corriente");
                writer.println("103|3|2100.75|Ahorros");
            }
        }
        
        try (BufferedReader reader = new BufferedReader(new FileReader(cuentasFile))) {
            String linea;
            while ((linea = reader.readLine()) != null) {
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
                }
            }
        }
    }
    
    /**
     * Carga las transacciones desde archivos
     */
    private static void cargarTransacciones() throws IOException {
        File transaccionesDir = new File(DATA_DIR + "/transacciones");
        if (!transaccionesDir.exists()) {
            log("Directorio de transacciones no encontrado. Creando directorio...");
            transaccionesDir.mkdirs();
        }
        
        File transaccionesFile = new File(DATA_DIR + "/transacciones/transacciones.txt");
        if (!transaccionesFile.exists()) {
            log("Archivo de transacciones no encontrado. Creando archivo vacío...");
            transaccionesFile.createNewFile();
        } else {
            try (BufferedReader reader = new BufferedReader(new FileReader(transaccionesFile))) {
                String linea;
                while ((linea = reader.readLine()) != null) {
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
    }
    
    /**
     * Guarda las transacciones en el archivo
     */
    private static void guardarTransacciones() throws IOException {
        File transaccionesFile = new File(DATA_DIR + "/transacciones/transacciones.txt");
        
        try (PrintWriter writer = new PrintWriter(transaccionesFile)) {
            for (Map<String, Object> transaccion : transacciones) {
                writer.println(
                    transaccion.get("id_transacc") + "|" +
                    transaccion.get("id_orig") + "|" +
                    transaccion.get("id_dest") + "|" +
                    transaccion.get("monto") + "|" +
                    transaccion.get("fecha_hora") + "|" +
                    transaccion.get("estado")
                );
            }
        }
    }
    
    /**
     * Guarda las cuentas en el archivo
     */
    private static void guardarCuentas() throws IOException {
        File cuentasFile = new File(DATA_DIR + "/cuentas/cuentas.txt");
        
        try (PrintWriter writer = new PrintWriter(cuentasFile)) {
            for (Map.Entry<Integer, Map<String, Object>> entry : cuentas.entrySet()) {
                int idCuenta = entry.getKey();
                Map<String, Object> cuenta = entry.getValue();
                
                writer.println(
                    idCuenta + "|" +
                    cuenta.get("id_cliente") + "|" +
                    cuenta.get("saldo") + "|" +
                    cuenta.get("tipo_cuenta")
                );
            }
        }
    }
    
    /**
     * Inicia el servidor para escuchar solicitudes
     */
    private static void iniciarServidor() {
        log("Iniciando servidor en puerto " + PUERTO);
        
        try (ServerSocket serverSocket = new ServerSocket(PUERTO)) {
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
            log("Solicitud recibida: " + solicitud);
            
            // Verificar que la solicitud no sea nula
            if (solicitud == null) {
                log("Solicitud nula recibida, podría ser un health check o una conexión cerrada");
                return;
            }
            
            // Parsear la solicitud
            // Formato esperado: TASK|idTarea|operacion|param1|param2|...
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
            
            // Enviar respuesta
            // Formato: RESPONSE|idTarea|resultado
            String respuesta = "RESPONSE|" + idTarea + "|" + resultado;
            salida.println(respuesta);
            log("Respuesta enviada: " + respuesta);
            
        } catch (Exception e) {
            log("Error procesando solicitud: " + e.getMessage());
            e.printStackTrace();
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
            
            // Obtener el saldo (con lock para evitar condiciones de carrera)
            ReentrantLock lock = cuentaLocks.get(idCuenta);
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
            
            ReentrantLock firstLock = cuentaOrigen < cuentaDestino ? lockOrigen : lockDestino;
            ReentrantLock secondLock = cuentaOrigen < cuentaDestino ? lockDestino : lockOrigen;
            
            firstLock.lock();
            try {
                secondLock.lock();
                try {
                    // Verificar saldo suficiente
                    double saldoOrigen = (double) cuentas.get(cuentaOrigen).get("saldo");
                    
                    if (saldoOrigen < monto) {
                        return "ERROR|Saldo insuficiente";
                    }
                    
                    // Realizar la transferencia
                    cuentas.get(cuentaOrigen).put("saldo", saldoOrigen - monto);
                    
                    double saldoDestino = (double) cuentas.get(cuentaDestino).get("saldo");
                    cuentas.get(cuentaDestino).put("saldo", saldoDestino + monto);
                    
                    // Registrar la transacción
                    transaccionLock.lock();
                    try {
                        // Generar ID de transacción (simplificado)
                        int idTransaccion = transacciones.size() + 1;
                        
                        // Crear registro de transacción
                        Map<String, Object> transaccion = new HashMap<>();
                        transaccion.put("id_transacc", idTransaccion);
                        transaccion.put("id_orig", cuentaOrigen);
                        transaccion.put("id_dest", cuentaDestino);
                        transaccion.put("monto", monto);
                        transaccion.put("fecha_hora", formatoFecha.format(new Date()));
                        transaccion.put("estado", "Confirmada");
                        
                        transacciones.add(transaccion);
                        
                        // Guardar cambios en archivos
                        guardarCuentas();
                        guardarTransacciones();
                        
                        return "OK|Transferencia completada";
                    } finally {
                        transaccionLock.unlock();
                    }
                    
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
     * Registra un mensaje de log
     */
    private static void log(String mensaje) {
        String timestamp = formatoFecha.format(new Date());
        String logLine = "[" + timestamp + "] " + mensaje;
        
        // Imprimir en consola
        System.out.println(logLine);
        
        // Escribir en archivo de log
        try {
            File logDir = new File("../logs");
            if (!logDir.exists()) {
                logDir.mkdirs();
            }
            
            try (FileWriter fw = new FileWriter(LOG_FILE, true);
                 BufferedWriter bw = new BufferedWriter(fw);
                 PrintWriter out = new PrintWriter(bw)) {
                out.println(logLine);
            }
        } catch (IOException e) {
            // Error silencioso - ya imprimimos en consola
            System.err.println("Error escribiendo en log: " + e.getMessage());
        }
    }
}
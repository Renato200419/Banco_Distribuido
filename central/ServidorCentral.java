import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.nio.file.*;
import java.text.DecimalFormat;

/**
 * ServidorCentral - Coordinador del sistema bancario distribuido
 * 
 * Este servidor recibe solicitudes de clientes y las distribuye
 * a nodos trabajadores para su procesamiento.
 */
public class ServidorCentral {
    // Puerto para recibir solicitudes de clientes
    private static final int PUERTO_SERVIDOR = 9000;
    
    // Configuración de nodos trabajadores
    private static List<NodoTrabajador> nodosTrabajadores = new ArrayList<>();
    
    // Mapa para almacenar tareas pendientes
    private static ConcurrentHashMap<Integer, InfoTarea> tareasPendientes = new ConcurrentHashMap<>();
    
    // Generador para IDs de tareas
    private static AtomicInteger generadorIdTarea = new AtomicInteger(1);
    
    // Para registro de actividad
    private static final SimpleDateFormat formatoFecha = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private static final String LOG_FILE = "../logs/servidor.log";
    
    // Directorios de datos
    private static final String DATA_DIR = "../data";
    
    // Saldo total para verificación
    private static double saldoTotal = 0;
    
    /**
     * Clase que representa un nodo trabajador
     */
    static class NodoTrabajador {
        String ip;
        int puerto;
        String lenguaje;
        boolean disponible;
        Set<String> particiones = new HashSet<>();
        
        public NodoTrabajador(String ip, int puerto, String lenguaje) {
            this.ip = ip;
            this.puerto = puerto;
            this.lenguaje = lenguaje;
            this.disponible = true;
        }
        
        @Override
        public String toString() {
            return "Nodo[ip=" + ip + ", puerto=" + puerto + ", lenguaje=" + lenguaje + 
                   ", disponible=" + disponible + "]";
        }
    }
    
    /**
     * Clase que almacena información de una tarea
     */
    static class InfoTarea {
        int idTarea;
        String operacion;
        String[] parametros;
        CompletableFuture<String> resultadoFuturo;
        
        public InfoTarea(int idTarea, String operacion, String[] parametros) {
            this.idTarea = idTarea;
            this.operacion = operacion;
            this.parametros = parametros;
            this.resultadoFuturo = new CompletableFuture<>();
        }
    }
    
    /**
     * Método principal
     */
    public static void main(String[] args) {
        try {
            // Crear directorios si no existen
            crearDirectorios();
            
            // Inicializar datos
            inicializarDatos();
            
            // Cargar la configuración de nodos trabajadores
            cargarConfiguracionNodos();
            
            // Iniciar monitor de estado de nodos
            iniciarMonitorNodos();
            
            // Iniciar monitor de arqueo
            iniciarMonitorArqueo();
            
            // Pool de hilos para manejar solicitudes de clientes
            ExecutorService poolHilosClientes = Executors.newFixedThreadPool(50);
            
            log("Servidor Central iniciado en puerto " + PUERTO_SERVIDOR);
            log("Nodos trabajadores configurados: " + nodosTrabajadores.size());
            log("Saldo total inicial: " + new DecimalFormat("#,##0.00").format(saldoTotal));
            
            try (ServerSocket socketServidor = new ServerSocket(PUERTO_SERVIDOR)) {
                while (true) {
                    try {
                        // Esperar conexión de cliente
                        Socket socketCliente = socketServidor.accept();
                        
                        // Procesar solicitud en un hilo separado
                        poolHilosClientes.submit(() -> manejarSolicitudCliente(socketCliente));
                        
                    } catch (IOException e) {
                        log("Error aceptando conexión: " + e.getMessage());
                    }
                }
            } catch (IOException e) {
                log("Error crítico en el servidor: " + e.getMessage());
                e.printStackTrace();
            }
        } catch (Exception e) {
            System.err.println("Error inicializando el servidor: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    /**
     * Crea los directorios necesarios para el sistema
     */
    private static void crearDirectorios() {
        try {
            // Directorios para datos
            Files.createDirectories(Paths.get(DATA_DIR + "/clientes"));
            Files.createDirectories(Paths.get(DATA_DIR + "/cuentas"));
            Files.createDirectories(Paths.get(DATA_DIR + "/transacciones"));
            
            // Directorio para logs
            Files.createDirectories(Paths.get("../logs"));
        } catch (IOException e) {
            System.err.println("Error creando directorios: " + e.getMessage());
        }
    }
    
    /**
     * Inicializa los datos del sistema
     */
    private static void inicializarDatos() {
        // Verificar si existen archivos de datos
        File clientesFile = new File(DATA_DIR + "/clientes/clientes.txt");
        File cuentasFile = new File(DATA_DIR + "/cuentas/cuentas.txt");
        
        if (!clientesFile.exists() || !cuentasFile.exists()) {
            // Crear datos de prueba
            crearDatosPrueba();
        }
        
        // Calcular saldo total
        calcularSaldoTotal();
    }
    
    /**
     * Crea datos de prueba para el sistema
     */
    private static void crearDatosPrueba() {
        try {
            // Crear clientes de prueba
            try (BufferedWriter writer = new BufferedWriter(new FileWriter(DATA_DIR + "/clientes/clientes.txt"))) {
                writer.write("1|Juan Pérez|juan@email.com|987654321\n");
                writer.write("2|María López|maria@email.com|998877665\n");
                writer.write("3|Carlos Rodríguez|carlos@email.com|912345678\n");
                writer.write("4|Ana Martínez|ana@email.com|923456789\n");
                writer.write("5|Pedro Sánchez|pedro@email.com|934567890\n");
            }
            
            // Crear cuentas de prueba
            try (BufferedWriter writer = new BufferedWriter(new FileWriter(DATA_DIR + "/cuentas/cuentas.txt"))) {
                writer.write("101|1|1500.00|Ahorros\n");
                writer.write("102|2|3200.50|Corriente\n");
                writer.write("103|3|2100.75|Ahorros\n");
                writer.write("104|4|5000.00|Corriente\n");
                writer.write("105|5|750.25|Ahorros\n");
            }
            
            // Crear archivo de transacciones vacío
            try (BufferedWriter writer = new BufferedWriter(new FileWriter(DATA_DIR + "/transacciones/transacciones.txt"))) {
                writer.write(""); // Archivo vacío inicialmente
            }
            
            log("Datos de prueba creados exitosamente");
        } catch (IOException e) {
            log("Error creando datos de prueba: " + e.getMessage());
        }
    }
    
    /**
     * Calcula el saldo total de todas las cuentas
     */
    private static void calcularSaldoTotal() {
        saldoTotal = 0;
        
        try (BufferedReader reader = new BufferedReader(new FileReader(DATA_DIR + "/cuentas/cuentas.txt"))) {
            String linea;
            while ((linea = reader.readLine()) != null) {
                String[] partes = linea.split("\\|");
                if (partes.length >= 3) {
                    try {
                        double saldo = Double.parseDouble(partes[2]);
                        saldoTotal += saldo;
                    } catch (NumberFormatException e) {
                        log("Error parseando saldo: " + e.getMessage());
                    }
                }
            }
        } catch (IOException e) {
            log("Error calculando saldo total: " + e.getMessage());
        }
    }
    
    /**
     * Carga la configuración de los nodos trabajadores
     */


    /*
    private static void cargarConfiguracionNodos() {
        // Por ahora configuramos nodos de forma estática
        // En un sistema real, esto se cargaría desde un archivo de configuración
        
        // Solicitar IPs de los nodos a los demás integrantes
        // IMPORTANTE: Actualizar estas IPs con las de tus compañeros
        
        // Nodo 1 (Python) - Actualizar con la IP real
        NodoTrabajador nodo1 = new NodoTrabajador("192.168.10.101", 9101, "python");
        nodo1.particiones.addAll(Arrays.asList("parte1.1", "parte2.1", "parte2.2", "parte2.3"));
        
        // Nodo 2 (JavaScript) - Actualizar con la IP real
        NodoTrabajador nodo2 = new NodoTrabajador("192.168.10.102", 9102, "javascript");
        nodo2.particiones.addAll(Arrays.asList("parte1.1", "parte1.2", "parte2.2", "parte2.3", "parte2.4"));
        
        // Nodo 3 (TypeScript) - Actualizar con la IP real
        NodoTrabajador nodo3 = new NodoTrabajador("192.168.10.103", 9103, "typescript");
        nodo3.particiones.addAll(Arrays.asList("parte1.1", "parte1.2", "parte1.3", "parte2.3", "parte2.4"));
        
        // Agregar nodos a la lista
        nodosTrabajadores.add(nodo1);
        nodosTrabajadores.add(nodo2);
        nodosTrabajadores.add(nodo3);
        
        log("Nodos trabajadores configurados: " + nodosTrabajadores);
    }
    */

    // Para pruebas:
    private static void cargarConfiguracionNodos() {
        // Todos los nodos en localhost para pruebas
        NodoTrabajador nodo1 = new NodoTrabajador("127.0.0.1", 9101, "python");
        nodo1.particiones.addAll(Arrays.asList("parte1.1", "parte2.1", "parte2.2", "parte2.3"));
        
        NodoTrabajador nodo2 = new NodoTrabajador("127.0.0.1", 9102, "javascript");
        nodo2.particiones.addAll(Arrays.asList("parte1.1", "parte1.2", "parte2.2", "parte2.3", "parte2.4"));
        
        NodoTrabajador nodo3 = new NodoTrabajador("127.0.0.1", 9103, "typescript");
        nodo3.particiones.addAll(Arrays.asList("parte1.1", "parte1.2", "parte1.3", "parte2.3", "parte2.4"));
        
        // Agregar nodos a la lista
        nodosTrabajadores.add(nodo1);
        nodosTrabajadores.add(nodo2);
        nodosTrabajadores.add(nodo3);
        
        log("Nodos trabajadores configurados: " + nodosTrabajadores);
    }    





    /**
     * Inicia el monitor de estado de nodos
     */
    private static void iniciarMonitorNodos() {
        Thread hiloMonitor = new Thread(() -> {
            while (true) {
                for (NodoTrabajador nodo : nodosTrabajadores) {
                    // Verificar si el nodo está vivo
                    boolean estaVivo = verificarEstadoNodo(nodo);
                    
                    if (nodo.disponible != estaVivo) {
                        nodo.disponible = estaVivo;
                        log("Estado del nodo " + nodo.ip + ":" + nodo.puerto + " cambiado a " + 
                            (estaVivo ? "DISPONIBLE" : "NO DISPONIBLE"));
                    }
                }
                
                // Verificar cada 5 segundos
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        });
        
        hiloMonitor.setDaemon(true);
        hiloMonitor.start();
    }
    
    /**
     * Inicia el monitor de arqueo (verificación de saldo total)
     */
    private static void iniciarMonitorArqueo() {
        Thread hiloArqueo = new Thread(() -> {
            while (true) {
                // Calcular saldo total actual
                double saldoAnterior = saldoTotal;
                calcularSaldoTotal();
                
                if (Math.abs(saldoAnterior - saldoTotal) > 0.001) {
                    log("ALERTA: Cambio en saldo total detectado!");
                    log("Saldo anterior: " + new DecimalFormat("#,##0.00").format(saldoAnterior));
                    log("Saldo actual: " + new DecimalFormat("#,##0.00").format(saldoTotal));
                }
                
                // Verificar cada 30 segundos
                try {
                    Thread.sleep(30000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        });
        
        hiloArqueo.setDaemon(true);
        hiloArqueo.start();
    }
    
    /**
     * Verifica si un nodo está vivo
     */
    private static boolean verificarEstadoNodo(NodoTrabajador nodo) {
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(nodo.ip, nodo.puerto), 1000);
            return true;
        } catch (IOException e) {
            return false;
        }
    }
    
    /**
     * Maneja una solicitud de cliente
     */
    private static void manejarSolicitudCliente(Socket socketCliente) {
        try (
            BufferedReader entrada = new BufferedReader(new InputStreamReader(socketCliente.getInputStream()));
            PrintWriter salida = new PrintWriter(socketCliente.getOutputStream(), true)
        ) {
            // Leer solicitud
            String solicitud = entrada.readLine();
            
            if (solicitud == null) {
                salida.println("ERROR|No se recibió ninguna solicitud");
                return;
            }
            
            log("Solicitud recibida: " + solicitud);
            
            // Parsing de la solicitud
            String[] partes = solicitud.split("\\|");
            
            if (partes.length < 3 || !partes[0].equals("REQUEST")) {
                salida.println("ERROR|Formato de solicitud inválido");
                return;
            }
            
            String operacion = partes[1];
            String[] parametros = Arrays.copyOfRange(partes, 2, partes.length);
            
            // Crear tarea y asignarla a un nodo trabajador
            int idTarea = generadorIdTarea.getAndIncrement();
            InfoTarea tarea = new InfoTarea(idTarea, operacion, parametros);
            tareasPendientes.put(idTarea, tarea);
            
            log("Tarea creada: ID=" + idTarea + ", Op=" + operacion + ", Params=" + Arrays.toString(parametros));
            
            // Enviar tarea a un nodo trabajador
            enviarTareaANodo(tarea);
            
            // Esperar resultado (con timeout)
            try {
                String resultado = tarea.resultadoFuturo.get(30, TimeUnit.SECONDS);
                salida.println(resultado);
                log("Resultado de tarea " + idTarea + ": " + resultado);
                
                // Si fue una transferencia exitosa, actualizar saldo total
                if (operacion.equals("TRANSFERIR_FONDOS") && resultado.startsWith("OK")) {
                    calcularSaldoTotal();
                }
            } catch (Exception e) {
                salida.println("ERROR|Tiempo de espera agotado o error en procesamiento");
                log("Error esperando resultado de tarea " + idTarea + ": " + e.getMessage());
                tareasPendientes.remove(idTarea);
            }
            
        } catch (IOException e) {
            log("Error manejando solicitud del cliente: " + e.getMessage());
        }
    }
    
    /**
     * Envía una tarea a un nodo trabajador apropiado
     */
    private static void enviarTareaANodo(InfoTarea tarea) {
        // Seleccionar nodo apropiado según la operación y particiones
        NodoTrabajador nodoSeleccionado = seleccionarNodo(tarea);
        
        if (nodoSeleccionado != null) {
            // Crear un hilo para enviar la tarea al nodo
            Thread hiloTrabajador = new Thread(() -> {
                try (
                    Socket socketNodo = new Socket(nodoSeleccionado.ip, nodoSeleccionado.puerto);
                    PrintWriter salida = new PrintWriter(socketNodo.getOutputStream(), true);
                    BufferedReader entrada = new BufferedReader(new InputStreamReader(socketNodo.getInputStream()))
                ) {
                    // Formato: TASK|idTarea|operacion|param1|param2|...
                    StringBuilder sb = new StringBuilder("TASK|" + tarea.idTarea + "|" + tarea.operacion);
                    for (String param : tarea.parametros) {
                        sb.append("|").append(param);
                    }
                    
                    String mensajeTarea = sb.toString();
                    log("Enviando a nodo " + nodoSeleccionado.ip + ":" + nodoSeleccionado.puerto + ": " + mensajeTarea);
                    
                    salida.println(mensajeTarea);
                    
                    // Leer respuesta
                    String respuesta = entrada.readLine();
                    log("Respuesta recibida de nodo: " + respuesta);
                    
                    // Completar el futuro con la respuesta
                    tarea.resultadoFuturo.complete(respuesta);
                    
                } catch (IOException e) {
                    log("Error conectando con nodo trabajador: " + e.getMessage());
                    
                    // Marcar el nodo como no disponible
                    nodoSeleccionado.disponible = false;
                    
                    // Intentar con otro nodo
                    enviarTareaANodo(tarea);
                }
            });
            
            hiloTrabajador.start();
        } else {
            // No hay nodos disponibles
            tarea.resultadoFuturo.complete("ERROR|No hay nodos trabajadores disponibles");
            log("No hay nodos disponibles para procesar tarea " + tarea.idTarea);
        }
    }
    
    /**
     * Selecciona un nodo apropiado para la tarea
     */
    private static NodoTrabajador seleccionarNodo(InfoTarea tarea) {
        // Determinar qué partición necesita la tarea
        String particionRequerida = null;
        
        if (tarea.operacion.equals("CONSULTAR_SALDO")) {
            int idCuenta = Integer.parseInt(tarea.parametros[0]);
            // Lógica simplificada para determinar la partición
            particionRequerida = "parte" + (idCuenta % 3 + 1) + "." + (idCuenta % 2 + 1);
            
        } else if (tarea.operacion.equals("TRANSFERIR_FONDOS")) {
            int cuentaOrigen = Integer.parseInt(tarea.parametros[0]);
            // Simplificado: usamos la cuenta origen para determinar la partición
            particionRequerida = "parte" + (cuentaOrigen % 3 + 1) + "." + (cuentaOrigen % 2 + 1);
        }
        
        log("Partición requerida para tarea " + tarea.idTarea + ": " + particionRequerida);
        
        // Buscar un nodo disponible que tenga esta partición
        if (particionRequerida != null) {
            for (NodoTrabajador nodo : nodosTrabajadores) {
                if (nodo.disponible && nodo.particiones.contains(particionRequerida)) {
                    log("Nodo seleccionado para tarea " + tarea.idTarea + ": " + nodo);
                    return nodo;
                }
            }
        }
        
        // Si no encontramos un nodo específico, seleccionar cualquiera disponible
        for (NodoTrabajador nodo : nodosTrabajadores) {
            if (nodo.disponible) {
                log("Nodo alternativo seleccionado para tarea " + tarea.idTarea + ": " + nodo);
                return nodo;
            }
        }
        
        log("No se encontró ningún nodo disponible para tarea " + tarea.idTarea);
        return null;
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
        try (FileWriter fw = new FileWriter(LOG_FILE, true);
             BufferedWriter bw = new BufferedWriter(fw);
             PrintWriter out = new PrintWriter(bw)) {
            out.println(logLine);
        } catch (IOException e) {
            // Error silencioso - ya imprimimos en consola
            System.err.println("Error escribiendo en log: " + e.getMessage());
        }
    }
}
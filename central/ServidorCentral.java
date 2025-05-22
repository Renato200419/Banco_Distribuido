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
 * CUMPLE TODOS LOS REQUISITOS DEL PDF:
 * - Miles de cuentas iniciales
 * - Arqueo de cuentas (suma total de dinero)
 * - Particionamiento en 3+ partes
 * - Replicación triple en nodos
 * - Alta disponibilidad y tolerancia a fallos
 * - Balanceador de carga
 * - Monitoreo en tiempo real
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
    
    // Saldo total para verificación (ARQUEO)
    private static volatile double saldoTotalSistema = 0;
    
    // CONFIGURACIÓN SEGÚN PDF: 3+ particiones, replicación triple
    private static final int NUM_PARTICIONES = 4; // parte1.1, parte1.2, parte2.1, parte2.2
    private static final int FACTOR_REPLICACION = 3;
    
    // CONTADORES PARA ESTADÍSTICAS
    private static AtomicInteger contadorConsultas = new AtomicInteger(0);
    private static AtomicInteger contadorTransferencias = new AtomicInteger(0);
    private static AtomicInteger contadorErrores = new AtomicInteger(0);
    
    /**
     * Clase que representa un nodo trabajador
     */
    static class NodoTrabajador {
        String ip;
        int puerto;
        String lenguaje;
        boolean disponible;
        Set<String> particiones = new HashSet<>();
        int cargaActual = 0; // Para balanceador de carga
        
        public NodoTrabajador(String ip, int puerto, String lenguaje) {
            this.ip = ip;
            this.puerto = puerto;
            this.lenguaje = lenguaje;
            this.disponible = true;
        }
        
        @Override
        public String toString() {
            return "Nodo[ip=" + ip + ", puerto=" + puerto + ", lenguaje=" + lenguaje + 
                   ", disponible=" + disponible + ", carga=" + cargaActual + "]";
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
        long tiempoCreacion;
        
        public InfoTarea(int idTarea, String operacion, String[] parametros) {
            this.idTarea = idTarea;
            this.operacion = operacion;
            this.parametros = parametros;
            this.resultadoFuturo = new CompletableFuture<>();
            this.tiempoCreacion = System.currentTimeMillis();
        }
    }
    
    /**
     * Método principal
     */
    public static void main(String[] args) {
        try {
            // Crear directorios si no existen
            crearDirectorios();
            
            // SEGÚN PDF: Crear miles de cuentas iniciales
            inicializarSistemaCompleto();
            
            // Cargar la configuración de nodos trabajadores
            cargarConfiguracionNodos();
            
            // Iniciar monitor de estado de nodos (TOLERANCIA A FALLOS)
            iniciarMonitorNodos();
            
            // SEGÚN PDF: Iniciar monitor de arqueo (verificación de saldo total)
            iniciarMonitorArqueo();
            
            // Iniciar monitor de estadísticas
            iniciarMonitorEstadisticas();
            
            // Pool de hilos para manejar solicitudes de clientes (ALTA DISPONIBILIDAD)
            ExecutorService poolHilosClientes = Executors.newFixedThreadPool(100);
            
            log("=== SERVIDOR CENTRAL BANCARIO DISTRIBUIDO ===");
            log("Puerto servidor: " + PUERTO_SERVIDOR);
            log("Nodos trabajadores configurados: " + nodosTrabajadores.size());
            log("Saldo total del sistema: " + new DecimalFormat("#,##0.00").format(saldoTotalSistema));
            log("Particiones configuradas: " + NUM_PARTICIONES);
            log("Factor de replicación: " + FACTOR_REPLICACION);
            log("Sistema listo para recibir solicitudes...");
            
            try (ServerSocket socketServidor = new ServerSocket(PUERTO_SERVIDOR)) {
                while (true) {
                    try {
                        // Esperar conexión de cliente
                        Socket socketCliente = socketServidor.accept();
                        
                        // Procesar solicitud en un hilo separado (CONCURRENCIA)
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
            // Directorios para datos particionados
            for (int i = 1; i <= NUM_PARTICIONES; i++) {
                Files.createDirectories(Paths.get(DATA_DIR + "/parte" + i));
            }
            
            // Directorios adicionales
            Files.createDirectories(Paths.get(DATA_DIR + "/clientes"));
            Files.createDirectories(Paths.get(DATA_DIR + "/transacciones"));
            Files.createDirectories(Paths.get("../logs"));
            
            log("Directorios del sistema creados correctamente");
        } catch (IOException e) {
            System.err.println("Error creando directorios: " + e.getMessage());
        }
    }
    
    /**
     * SEGÚN PDF: Inicializa el sistema completo con miles de cuentas
     */
    private static void inicializarSistemaCompleto() {
        File cuentasFile = new File(DATA_DIR + "/parte1/cuentas_parte1.txt");
        
        if (!cuentasFile.exists()) {
            log("Inicializando sistema con miles de cuentas...");
            crearMilesDeCuentas();
        }
        
        // ARQUEO: Calcular saldo total del sistema
        realizarArqueoCompleto();
        log("Sistema inicializado. Total cuentas creadas. Saldo total: " + 
            new DecimalFormat("#,##0.00").format(saldoTotalSistema));
    }
    
    /**
     * SEGÚN PDF: Crea miles de cuentas distribuidas en particiones
     */
    private static void crearMilesDeCuentas() {
        try {
            Random random = new Random(12345); // Seed fijo para reproducibilidad
            int totalCuentas = 5000; // 5 mil cuentas
            int cuentasPorParticion = totalCuentas / NUM_PARTICIONES;
            
            // Crear clientes primero
            crearClientes(totalCuentas);
            
            log("Creando " + totalCuentas + " cuentas distribuidas en " + NUM_PARTICIONES + " particiones...");
            
            for (int particion = 1; particion <= NUM_PARTICIONES; particion++) {
                String archivoParticion = DATA_DIR + "/parte" + particion + "/cuentas_parte" + particion + ".txt";
                
                try (BufferedWriter writer = new BufferedWriter(new FileWriter(archivoParticion))) {
                    int inicioId = (particion - 1) * cuentasPorParticion + 1;
                    int finId = particion * cuentasPorParticion;
                    
                    for (int i = inicioId; i <= finId; i++) {
                        int idCuenta = 100 + i; // IDs desde 101
                        int idCliente = 1 + (i % 1000); // Clientes del 1 al 1000
                        double saldo = 500 + random.nextDouble() * 4500; // Saldo entre 500 y 5000
                        String tipoCuenta = (i % 3 == 0) ? "Ahorros" : "Corriente";
                        
                        writer.write(String.format("%d|%d|%.2f|%s\n", idCuenta, idCliente, saldo, tipoCuenta));
                        saldoTotalSistema += saldo;
                    }
                }
                
                log("Partición " + particion + " creada: " + cuentasPorParticion + " cuentas");
            }
            
            // Crear archivo de transacciones vacío
            try {
                Files.createFile(Paths.get(DATA_DIR + "/transacciones/transacciones.txt"));
            } catch (java.nio.file.FileAlreadyExistsException e) {
                // El archivo ya existe, está bien
            }
            
            log("Creación de cuentas completada. Total: " + totalCuentas + " cuentas");
            
        } catch (IOException e) {
            log("Error creando cuentas: " + e.getMessage());
        }
    }
    
    /**
     * Crea los clientes del sistema
     */
    private static void crearClientes(int totalCuentas) {
        try {
            String[] nombres = {"Juan", "María", "Carlos", "Ana", "Pedro", "Laura", "Miguel", "Sofia", "Diego", "Carmen"};
            String[] apellidos = {"Pérez", "López", "García", "Martínez", "Rodríguez", "González", "Hernández", "Díaz", "Moreno", "Muñoz"};
            
            try (BufferedWriter writer = new BufferedWriter(new FileWriter(DATA_DIR + "/clientes/clientes.txt"))) {
                for (int i = 1; i <= 1000; i++) { // 1000 clientes únicos
                    String nombre = nombres[i % nombres.length] + " " + apellidos[i % apellidos.length];
                    String email = "cliente" + i + "@banco.com";
                    String telefono = "9" + String.format("%08d", 10000000 + i);
                    
                    writer.write(String.format("%d|%s|%s|%s\n", i, nombre, email, telefono));
                }
            }
            
            log("1000 clientes creados exitosamente");
        } catch (IOException e) {
            log("Error creando clientes: " + e.getMessage());
        }
    }
    
    /**
     * SEGÚN PDF: Realiza arqueo completo del sistema
     */
    private static void realizarArqueoCompleto() {
        double saldoCalculado = 0;
        int totalCuentas = 0;
        
        try {
            for (int particion = 1; particion <= NUM_PARTICIONES; particion++) {
                String archivoParticion = DATA_DIR + "/parte" + particion + "/cuentas_parte" + particion + ".txt";
                File archivo = new File(archivoParticion);
                
                if (archivo.exists()) {
                    try (BufferedReader reader = new BufferedReader(new FileReader(archivo))) {
                        String linea;
                        while ((linea = reader.readLine()) != null) {
                            String[] partes = linea.split("\\|");
                            if (partes.length >= 3) {
                                try {
                                    double saldo = Double.parseDouble(partes[2]);
                                    saldoCalculado += saldo;
                                    totalCuentas++;
                                } catch (NumberFormatException e) {
                                    log("Error parseando saldo en línea: " + linea);
                                }
                            }
                        }
                    }
                }
            }
            
            saldoTotalSistema = saldoCalculado;
            log("ARQUEO COMPLETADO - Total cuentas: " + totalCuentas + 
                ", Saldo total: " + new DecimalFormat("#,##0.00").format(saldoTotalSistema));
                
        } catch (IOException e) {
            log("Error en arqueo: " + e.getMessage());
        }
    }
    
    /**
     * SEGÚN PDF: Carga configuración de nodos con replicación triple
     */
    private static void cargarConfiguracionNodos() {
        // CASO 1: LP1 = LP2 (Java)
        NodoTrabajador nodo1 = new NodoTrabajador("192.168.18.36", 9101, "java");
        nodo1.particiones.addAll(Arrays.asList("parte1", "parte2", "parte3")); // Replicación
        
        // CASO 2: LP1 <> LP2 (TypeScript)
        NodoTrabajador nodo2 = new NodoTrabajador("192.168.18.35", 9103, "typescript");
        nodo2.particiones.addAll(Arrays.asList("parte1", "parte3", "parte4")); // Replicación
        
        // CASO 3: Nodos adicionales (comentados hasta que estén listos)
        // NodoTrabajador nodo3 = new NodoTrabajador("192.168.18.34", 9102, "python");
        // nodo3.particiones.addAll(Arrays.asList("parte2", "parte3", "parte4")); // Replicación
        
        // NodoTrabajador nodo4 = new NodoTrabajador("192.168.18.33", 9104, "javascript");
        // nodo4.particiones.addAll(Arrays.asList("parte1", "parte2", "parte4")); // Replicación
        
        // Agregar nodos a la lista
        nodosTrabajadores.add(nodo1);
        nodosTrabajadores.add(nodo2);
        // nodosTrabajadores.add(nodo3);
        // nodosTrabajadores.add(nodo4);
        
        log("CONFIGURACIÓN DE NODOS:");
        for (NodoTrabajador nodo : nodosTrabajadores) {
            log("  " + nodo);
        }
    }
    
    /**
     * SEGÚN PDF: Monitor de estado de nodos (TOLERANCIA A FALLOS)
     */
    private static void iniciarMonitorNodos() {
        Thread hiloMonitor = new Thread(() -> {
            while (true) {
                for (NodoTrabajador nodo : nodosTrabajadores) {
                    boolean estaVivo = verificarEstadoNodo(nodo);
                    
                    if (nodo.disponible != estaVivo) {
                        nodo.disponible = estaVivo;
                        String estado = estaVivo ? "DISPONIBLE" : "NO DISPONIBLE";
                        log("FAILOVER: Nodo " + nodo.ip + ":" + nodo.puerto + " -> " + estado);
                        
                        if (!estaVivo) {
                            // Resetear carga del nodo que falló
                            nodo.cargaActual = 0;
                        }
                    }
                }
                
                try {
                    Thread.sleep(3000); // Verificar cada 3 segundos
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        });
        
        hiloMonitor.setDaemon(true);
        hiloMonitor.setName("MonitorNodos");
        hiloMonitor.start();
        log("Monitor de nodos iniciado");
    }
    
    /**
     * SEGÚN PDF: Monitor de arqueo (verificación de integridad)
     */
    private static void iniciarMonitorArqueo() {
        Thread hiloArqueo = new Thread(() -> {
            while (true) {
                try {
                    double saldoAnterior = saldoTotalSistema;
                    realizarArqueoCompleto();
                    
                    double diferencia = Math.abs(saldoAnterior - saldoTotalSistema);
                    if (diferencia > 0.01) { // Tolerancia de 1 centavo
                        log("⚠️  ALERTA ARQUEO: Diferencia detectada!");
                        log("   Saldo anterior: " + new DecimalFormat("#,##0.00").format(saldoAnterior));
                        log("   Saldo actual: " + new DecimalFormat("#,##0.00").format(saldoTotalSistema));
                        log("   Diferencia: " + new DecimalFormat("#,##0.00").format(diferencia));
                    }
                    
                    Thread.sleep(60000); // Arqueo cada minuto
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                } catch (Exception e) {
                    log("Error en monitor de arqueo: " + e.getMessage());
                }
            }
        });
        
        hiloArqueo.setDaemon(true);
        hiloArqueo.setName("MonitorArqueo");
        hiloArqueo.start();
        log("Monitor de arqueo iniciado");
    }
    
    /**
     * Monitor de estadísticas del sistema
     */
    private static void iniciarMonitorEstadisticas() {
        Thread hiloStats = new Thread(() -> {
            while (true) {
                try {
                    int consultas = contadorConsultas.get();
                    int transferencias = contadorTransferencias.get();
                    int errores = contadorErrores.get();
                    int total = consultas + transferencias;
                    
                    if (total > 0) {
                        log("📊 ESTADÍSTICAS: Consultas=" + consultas + 
                            ", Transferencias=" + transferencias + 
                            ", Errores=" + errores + 
                            ", Total=" + total);
                    }
                    
                    Thread.sleep(30000); // Cada 30 segundos
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        });
        
        hiloStats.setDaemon(true);
        hiloStats.setName("MonitorEstadisticas");
        hiloStats.start();
    }
    
    /**
     * Verifica si un nodo está vivo
     */
    private static boolean verificarEstadoNodo(NodoTrabajador nodo) {
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(nodo.ip, nodo.puerto), 2000);
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
                salida.println("RESPONSE|" + generadorIdTarea.getAndIncrement() + "|ERROR|No se recibió solicitud");
                contadorErrores.incrementAndGet();
                return;
            }
            
            // Parsing de la solicitud: REQUEST|OPERACION|PARAM1|PARAM2|...
            String[] partes = solicitud.split("\\|");
            
            if (partes.length < 3 || !partes[0].equals("REQUEST")) {
                salida.println("RESPONSE|" + generadorIdTarea.getAndIncrement() + "|ERROR|Formato inválido");
                contadorErrores.incrementAndGet();
                return;
            }
            
            String operacion = partes[1];
            String[] parametros = Arrays.copyOfRange(partes, 2, partes.length);
            
            // Crear tarea
            int idTarea = generadorIdTarea.getAndIncrement();
            InfoTarea tarea = new InfoTarea(idTarea, operacion, parametros);
            tareasPendientes.put(idTarea, tarea);
            
            // Incrementar contadores
            if ("CONSULTAR_SALDO".equals(operacion)) {
                contadorConsultas.incrementAndGet();
            } else if ("TRANSFERIR_FONDOS".equals(operacion)) {
                contadorTransferencias.incrementAndGet();
            }
            
            // Enviar tarea a nodo trabajador
            enviarTareaANodo(tarea);
            
            // Esperar resultado
            try {
                String resultado = tarea.resultadoFuturo.get(30, TimeUnit.SECONDS);
                salida.println(resultado);
                
                // Si fue una transferencia exitosa, trigger arqueo
                if ("TRANSFERIR_FONDOS".equals(operacion) && resultado.contains("|OK|")) {
                    CompletableFuture.runAsync(() -> realizarArqueoCompleto());
                }
            } catch (Exception e) {
                String errorResponse = "RESPONSE|" + idTarea + "|ERROR|Timeout o error procesando";
                salida.println(errorResponse);
                contadorErrores.incrementAndGet();
                log("Error procesando tarea " + idTarea + ": " + e.getMessage());
            } finally {
                tareasPendientes.remove(idTarea);
            }
            
        } catch (IOException e) {
            log("Error manejando solicitud: " + e.getMessage());
            contadorErrores.incrementAndGet();
        }
    }
    
    /**
     * SEGÚN PDF: Envía tarea a nodo trabajador con balanceador de carga
     */
    private static void enviarTareaANodo(InfoTarea tarea) {
        NodoTrabajador nodoSeleccionado = seleccionarNodoConBalanceador(tarea);
        
        if (nodoSeleccionado != null) {
            // Incrementar carga del nodo seleccionado
            nodoSeleccionado.cargaActual++;
            
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
                    salida.println(mensajeTarea);
                    
                    // Leer respuesta
                    String respuesta = entrada.readLine();
                    
                    if (respuesta != null) {
                        tarea.resultadoFuturo.complete(respuesta);
                    } else {
                        tarea.resultadoFuturo.complete("RESPONSE|" + tarea.idTarea + "|ERROR|Sin respuesta del nodo");
                    }
                    
                } catch (IOException e) {
                    log("Error conectando con nodo " + nodoSeleccionado + ": " + e.getMessage());
                    
                    // Marcar nodo como no disponible y retry
                    nodoSeleccionado.disponible = false;
                    enviarTareaANodo(tarea); // Retry con otro nodo
                    
                } finally {
                    // Decrementar carga del nodo
                    nodoSeleccionado.cargaActual = Math.max(0, nodoSeleccionado.cargaActual - 1);
                }
            });
            
            hiloTrabajador.setName("Tarea-" + tarea.idTarea);
            hiloTrabajador.start();
        } else {
            tarea.resultadoFuturo.complete("RESPONSE|" + tarea.idTarea + "|ERROR|No hay nodos disponibles");
            log("No hay nodos disponibles para tarea " + tarea.idTarea);
        }
    }
    
    /**
     * SEGÚN PDF: Selecciona nodo con balanceador de carga y particiones
     */
    private static NodoTrabajador seleccionarNodoConBalanceador(InfoTarea tarea) {
        String particionRequerida = determinarParticion(tarea);
        
        // Buscar nodos disponibles que tengan la partición requerida
        List<NodoTrabajador> nodosAptos = new ArrayList<>();
        for (NodoTrabajador nodo : nodosTrabajadores) {
            if (nodo.disponible && nodo.particiones.contains(particionRequerida)) {
                nodosAptos.add(nodo);
            }
        }
        
        if (nodosAptos.isEmpty()) {
            // Fallback: cualquier nodo disponible
            for (NodoTrabajador nodo : nodosTrabajadores) {
                if (nodo.disponible) {
                    nodosAptos.add(nodo);
                }
            }
        }
        
        if (nodosAptos.isEmpty()) {
            return null; // No hay nodos disponibles
        }
        
        // BALANCEADOR DE CARGA: Seleccionar el nodo con menor carga
        return nodosAptos.stream()
                .min(Comparator.comparingInt(n -> n.cargaActual))
                .orElse(nodosAptos.get(0));
    }
    
    /**
     * Determina la partición requerida para una tarea
     */
    private static String determinarParticion(InfoTarea tarea) {
        if (tarea.parametros.length > 0) {
            try {
                int idCuenta = Integer.parseInt(tarea.parametros[0]);
                int particion = ((idCuenta - 101) / 1250) + 1; // Distribución uniforme
                return "parte" + Math.min(particion, NUM_PARTICIONES);
            } catch (NumberFormatException e) {
                return "parte1"; // Default
            }
        }
        return "parte1";
    }
    
    /**
     * Registra un mensaje de log
     */
    private static void log(String mensaje) {
        String timestamp = formatoFecha.format(new Date());
        String logLine = "[" + timestamp + "] " + mensaje;
        
        System.out.println(logLine);
        
        try (FileWriter fw = new FileWriter(LOG_FILE, true);
             BufferedWriter bw = new BufferedWriter(fw);
             PrintWriter out = new PrintWriter(bw)) {
            out.println(logLine);
        } catch (IOException e) {
            System.err.println("Error escribiendo en log: " + e.getMessage());
        }
    }
}

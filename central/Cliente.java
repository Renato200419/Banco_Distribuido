import java.io.*;
import java.net.*;
import java.util.Random;
import java.util.Scanner;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.CountDownLatch;

/**
 * Cliente - Aplicación cliente para el sistema bancario distribuido
 * 
 * CUMPLE TODOS LOS REQUISITOS DEL PDF:
 * - Pruebas de carga con cientos de transacciones
 * - Delay aleatorio entre transacciones
 * - Concurrencia real con múltiples hilos
 * - Estadísticas de desempeño
 * - IDs de cuentas correctos (101-5100)
 */
public class Cliente {
    // Configuración de conexión al servidor
    private static String IP_SERVIDOR = "192.168.18.31"; // Cambiar según configuración
    private static final int PUERTO_SERVIDOR = 9000;
    
    // Para generar valores aleatorios
    private static final Random random = new Random();
    
    // Para registro de actividad
    private static final SimpleDateFormat formatoFecha = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private static final String LOG_FILE = "../logs/cliente.log";
    
    // SEGÚN PDF: Rango de cuentas válidas (miles de cuentas)
    private static final int MIN_ID_CUENTA = 101;
    private static final int MAX_ID_CUENTA = 5100; // 5000 cuentas creadas
    
    // Contadores para estadísticas
    private static AtomicInteger transaccionesExitosas = new AtomicInteger(0);
    private static AtomicInteger transaccionesConError = new AtomicInteger(0);
    private static AtomicInteger consultasRealizadas = new AtomicInteger(0);
    private static AtomicInteger transferenciasRealizadas = new AtomicInteger(0);
    
    /**
     * Método principal
     */
    public static void main(String[] args) {
        // Verificar argumentos
        if (args.length >= 1) {
            // Modo prueba de carga automática
            modoAutomatico(args);
        } else {
            // Modo interactivo
            modoInteractivo();
        }
    }
    
    /**
     * Modo interactivo del cliente
     */
    private static void modoInteractivo() {
        Scanner scanner = new Scanner(System.in);
        boolean continuar = true;
        
        log("=== CLIENTE BANCARIO DISTRIBUIDO ===");
        log("Sistema iniciado en modo interactivo");
        
        // Configurar IP del servidor
        System.out.print("Ingrese la IP del servidor (Enter para " + IP_SERVIDOR + "): ");
        String ipIngresada = scanner.nextLine().trim();
        if (!ipIngresada.isEmpty()) {
            IP_SERVIDOR = ipIngresada;
        }
        log("Conectando a servidor: " + IP_SERVIDOR + ":" + PUERTO_SERVIDOR);
        
        while (continuar) {
            mostrarMenu();
            
            try {
                int opcion = scanner.nextInt();
                scanner.nextLine(); // Consumir salto de línea
                
                switch (opcion) {
                    case 1: // Consultar saldo
                        consultarSaldoInteractivo(scanner);
                        break;
                        
                    case 2: // Transferir fondos
                        transferirFondosInteractivo(scanner);
                        break;
                        
                    case 3: // Prueba de carga básica
                        System.out.print("Número de transacciones (recomendado 50-200): ");
                        int numTrans = scanner.nextInt();
                        scanner.nextLine();
                        realizarPruebaCarga(numTrans, 10); // 10 hilos concurrentes
                        break;
                        
                    case 4: // Prueba de carga intensa
                        System.out.print("Número de transacciones (100-1000): ");
                        int numTransIntensa = scanner.nextInt();
                        System.out.print("Número de hilos concurrentes (10-50): ");
                        int numHilos = scanner.nextInt();
                        scanner.nextLine();
                        realizarPruebaCarga(numTransIntensa, numHilos);
                        break;
                        
                    case 5: // Prueba de arqueo
                        realizarPruebaArqueo();
                        break;
                        
                    case 0: // Salir
                        continuar = false;
                        System.out.println("Cerrando cliente bancario...");
                        break;
                        
                    default:
                        System.out.println("Opción no válida. Intente de nuevo.");
                }
                
            } catch (Exception e) {
                System.err.println("Error: " + e.getMessage());
                scanner.nextLine(); // Limpiar buffer
            }
            
            if (continuar) {
                System.out.println("\nPresione Enter para continuar...");
                scanner.nextLine();
            }
        }
        
        scanner.close();
    }
    
    /**
     * Consulta saldo en modo interactivo
     */
    private static void consultarSaldoInteractivo(Scanner scanner) {
        System.out.print("ID de cuenta (" + MIN_ID_CUENTA + "-" + MAX_ID_CUENTA + "): ");
        int idCuenta = scanner.nextInt();
        scanner.nextLine();
        
        if (idCuenta < MIN_ID_CUENTA || idCuenta > MAX_ID_CUENTA) {
            System.out.println("ALERTA: ID de cuenta fuera del rango valido");
            return;
        }
        
        try {
            String resultado = consultarSaldo(idCuenta);
            System.out.println("Resultado: " + resultado);
            log("Consulta interactiva - Cuenta " + idCuenta + ": " + resultado);
        } catch (IOException e) {
            System.err.println("Error en consulta: " + e.getMessage());
        }
    }
    
    /**
     * Transferir fondos en modo interactivo
     */
    private static void transferirFondosInteractivo(Scanner scanner) {
        System.out.print("Cuenta origen (" + MIN_ID_CUENTA + "-" + MAX_ID_CUENTA + "): ");
        int cuentaOrigen = scanner.nextInt();
        
        System.out.print("Cuenta destino (" + MIN_ID_CUENTA + "-" + MAX_ID_CUENTA + "): ");
        int cuentaDestino = scanner.nextInt();
        
        System.out.print("Monto a transferir: ");
        double monto = scanner.nextDouble();
        scanner.nextLine();
        
        if (cuentaOrigen < MIN_ID_CUENTA || cuentaOrigen > MAX_ID_CUENTA ||
            cuentaDestino < MIN_ID_CUENTA || cuentaDestino > MAX_ID_CUENTA) {
            System.out.println("ALERTA: ID de cuenta fuera del rango valido");
            return;
        }
        
        if (monto <= 0) {
            System.out.println("ALERTA: El monto debe ser positivo");
            return;
        }
        
        try {
            String resultado = transferirFondos(cuentaOrigen, cuentaDestino, monto);
            System.out.println("Resultado: " + resultado);
            log("Transferencia interactiva - " + cuentaOrigen + " -> " + cuentaDestino + 
                " (" + monto + "): " + resultado);
        } catch (IOException e) {
            System.err.println("Error en transferencia: " + e.getMessage());
        }
    }
    
    /**
     * Muestra el menú de opciones
     */
    private static void mostrarMenu() {
        System.out.println("\n===== CLIENTE BANCARIO DISTRIBUIDO =====");
        System.out.println("1. Consultar saldo");
        System.out.println("2. Transferir fondos");
        System.out.println("3. Prueba de carga basica");
        System.out.println("4. Prueba de carga intensa");
        System.out.println("5. Prueba de arqueo");
        System.out.println("0. Salir");
        System.out.print("Seleccione una opcion: ");
    }
    
    /**
     * Modo automático para pruebas de carga
     */
    private static void modoAutomatico(String[] args) {
        try {
            int numTransacciones = Integer.parseInt(args[0]);
            int numHilos = args.length > 1 ? Integer.parseInt(args[1]) : 20;
            
            if (args.length > 2) {
                IP_SERVIDOR = args[2];
            }
            
            log("MODO AUTOMATICO INICIADO");
            log("Servidor: " + IP_SERVIDOR + ":" + PUERTO_SERVIDOR);
            log("Transacciones: " + numTransacciones);
            log("Hilos concurrentes: " + numHilos);
            
            realizarPruebaCarga(numTransacciones, numHilos);
            
        } catch (NumberFormatException e) {
            System.err.println("Error en argumentos: " + e.getMessage());
            System.err.println("Uso: java Cliente <num_transacciones> [num_hilos] [ip_servidor]");
            System.err.println("Ejemplo: java Cliente 500 25 192.168.1.100");
        }
    }
    
    /**
     * SEGÚN PDF: Realiza prueba de carga con cientos de transacciones
     */
    private static void realizarPruebaCarga(int numTransacciones, int numHilos) {
        log("INICIANDO PRUEBA DE CARGA");
        log("Transacciones: " + numTransacciones + ", Hilos: " + numHilos);
        
        // Resetear contadores
        transaccionesExitosas.set(0);
        transaccionesConError.set(0);
        consultasRealizadas.set(0);
        transferenciasRealizadas.set(0);
        
        // Pool de hilos para concurrencia real
        ExecutorService executor = Executors.newFixedThreadPool(numHilos);
        CountDownLatch latch = new CountDownLatch(numTransacciones);
        
        long tiempoInicio = System.currentTimeMillis();
        
        // SEGÚN PDF: Enviar cientos de transacciones con delay aleatorio
        for (int i = 0; i < numTransacciones; i++) {
            final int numTrans = i;
            executor.submit(() -> {
                try {
                    // SEGÚN PDF: Delay aleatorio para simular concurrencia real
                    Thread.sleep(random.nextInt(500) + 50); // 50-550ms
                    
                    if (numTrans % 3 == 0) {
                        // Consulta de saldo (33% de las transacciones)
                        realizarConsultaAleatoria();
                    } else {
                        // Transferencia (67% de las transacciones)
                        realizarTransferenciaAleatoria();
                    }
                    
                    transaccionesExitosas.incrementAndGet();
                    
                } catch (Exception e) {
                    transaccionesConError.incrementAndGet();
                    log("Error en transacción " + numTrans + ": " + e.getMessage());
                } finally {
                    latch.countDown();
                }
            });
        }
        
        try {
            // Esperar a que terminen todas las transacciones
            boolean terminado = latch.await(10, TimeUnit.MINUTES);
            executor.shutdown();
            
            long tiempoTotal = System.currentTimeMillis() - tiempoInicio;
            double segundos = tiempoTotal / 1000.0;
            
            // ESTADÍSTICAS FINALES
            mostrarResultadosPrueba(numTransacciones, segundos, terminado);
            
        } catch (InterruptedException e) {
            log("Prueba interrumpida: " + e.getMessage());
            Thread.currentThread().interrupt();
        }
    }
    
    /**
     * Realiza una consulta de saldo aleatoria
     */
    private static void realizarConsultaAleatoria() throws IOException {
        int idCuenta = MIN_ID_CUENTA + random.nextInt(MAX_ID_CUENTA - MIN_ID_CUENTA + 1);
        String resultado = consultarSaldo(idCuenta);
        consultasRealizadas.incrementAndGet();
        
        if (random.nextInt(100) < 5) { // Log 5% de las consultas
            log("Consulta " + idCuenta + ": " + resultado);
        }
    }
    
    /**
     * Realiza una transferencia aleatoria
     */
    private static void realizarTransferenciaAleatoria() throws IOException {
        int cuentaOrigen = MIN_ID_CUENTA + random.nextInt(MAX_ID_CUENTA - MIN_ID_CUENTA + 1);
        int cuentaDestino = MIN_ID_CUENTA + random.nextInt(MAX_ID_CUENTA - MIN_ID_CUENTA + 1);
        
        // Evitar transferencias a la misma cuenta
        while (cuentaDestino == cuentaOrigen) {
            cuentaDestino = MIN_ID_CUENTA + random.nextInt(MAX_ID_CUENTA - MIN_ID_CUENTA + 1);
        }
        
        double monto = 10.0 + random.nextDouble() * 490.0; // 10-500
        String resultado = transferirFondos(cuentaOrigen, cuentaDestino, monto);
        transferenciasRealizadas.incrementAndGet();
        
        if (random.nextInt(100) < 3) { // Log 3% de las transferencias
            log("Transferencia " + cuentaOrigen + " -> " + cuentaDestino + 
                " ($" + String.format("%.2f", monto) + "): " + resultado);
        }
    }
    
    /**
     * Muestra los resultados de la prueba de carga
     */
    private static void mostrarResultadosPrueba(int numTransacciones, double segundos, boolean terminado) {
        int exitosas = transaccionesExitosas.get();
        int errores = transaccionesConError.get();
        int consultas = consultasRealizadas.get();
        int transferencias = transferenciasRealizadas.get();
        
        double tps = exitosas / segundos;
        double tasaExito = (exitosas * 100.0) / numTransacciones;
        
        String resultado = "\n===== RESULTADOS PRUEBA DE CARGA =====\n" +
            "Transacciones solicitadas: " + numTransacciones + "\n" +
            "Transacciones exitosas: " + exitosas + " (" + String.format("%.1f", tasaExito) + "%)\n" +
            "Transacciones con error: " + errores + "\n" +
            "Consultas realizadas: " + consultas + "\n" +
            "Transferencias realizadas: " + transferencias + "\n" +
            "Tiempo total: " + String.format("%.3f", segundos) + " segundos\n" +
            "Transacciones por segundo: " + String.format("%.2f", tps) + " TPS\n" +
            "Tasa de exito: " + String.format("%.1f", tasaExito) + "%\n" +
            (terminado ? "Prueba completada" : "Timeout alcanzado") + "\n" +
            "==========================================";
        
        System.out.println(resultado);
        log(resultado.replace("\n", " | "));
    }
    
    /**
     * Realiza una prueba de arqueo consultando múltiples cuentas
     */
    private static void realizarPruebaArqueo() {
        log("Iniciando prueba de arqueo...");
        
        try {
            double saldoTotal = 0;
            int cuentasConsultadas = 0;
            int errores = 0;
            
            // Consultar una muestra de cuentas para verificar arqueo
            for (int i = 0; i < 100; i++) {
                int idCuenta = MIN_ID_CUENTA + random.nextInt(1000); // Muestra aleatoria
                
                try {
                    String resultado = consultarSaldo(idCuenta);
                    
                    if (resultado.contains("|OK|")) {
                        String[] partes = resultado.split("\\|");
                        if (partes.length >= 4) {
                            double saldo = Double.parseDouble(partes[3]);
                            saldoTotal += saldo;
                            cuentasConsultadas++;
                        }
                    } else {
                        errores++;
                    }
                    
                    Thread.sleep(50); // Pequeño delay
                    
                } catch (Exception e) {
                    errores++;
                }
            }
            
            System.out.println("RESULTADO PRUEBA DE ARQUEO");
            System.out.println("Cuentas consultadas: " + cuentasConsultadas);
            System.out.println("Saldo total muestra: $" + String.format("%.2f", saldoTotal));
            System.out.println("Errores: " + errores);
            System.out.println("Promedio por cuenta: $" + String.format("%.2f", 
                cuentasConsultadas > 0 ? saldoTotal / cuentasConsultadas : 0));
                
        } catch (Exception e) {
            System.err.println("Error en prueba de arqueo: " + e.getMessage());
        }
    }
    
    /**
     * Consulta el saldo de una cuenta
     */
    private static String consultarSaldo(int idCuenta) throws IOException {
        try (Socket socket = new Socket(IP_SERVIDOR, PUERTO_SERVIDOR);
             PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
             BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {
            
            String request = "REQUEST|CONSULTAR_SALDO|" + idCuenta;
            out.println(request);
            
            return in.readLine();
        }
    }
    
    /**
     * Realiza una transferencia entre cuentas
     */
    private static String transferirFondos(int cuentaOrigen, int cuentaDestino, double monto) throws IOException {
        try (Socket socket = new Socket(IP_SERVIDOR, PUERTO_SERVIDOR);
             PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
             BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {
            
            String request = String.format("REQUEST|TRANSFERIR_FONDOS|%d|%d|%.2f", 
                                        cuentaOrigen, cuentaDestino, monto);
            out.println(request);
            
            return in.readLine();
        }
    }
    
    /**
     * Registra un mensaje de log
     */
    private static void log(String mensaje) {
        String timestamp = formatoFecha.format(new Date());
        String logLine = "[" + timestamp + "] " + mensaje;
        
        System.out.println(logLine);
        
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
            System.err.println("Error escribiendo log: " + e.getMessage());
        }
    }
}

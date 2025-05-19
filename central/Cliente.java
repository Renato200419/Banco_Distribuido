import java.io.*;
import java.net.*;
import java.util.Random;
import java.util.Scanner;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Cliente - Aplicación cliente para el sistema bancario distribuido
 * 
 * Permite realizar consultas de saldo y transferencias, tanto de forma
 * interactiva como mediante pruebas de carga automatizadas.
 */
public class Cliente {
    // Configuración de conexión al servidor
    private static String IP_SERVIDOR = "localhost"; // Cambiar a la IP real del servidor
    private static final int PUERTO_SERVIDOR = 9000;
    
    // Para generar valores aleatorios
    private static final Random random = new Random();
    
    // Para registro de actividad
    private static final SimpleDateFormat formatoFecha = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private static final String LOG_FILE = "../logs/cliente.log";
    
    /**
     * Método principal
     */
    public static void main(String[] args) {
        // Verificar argumentos
        if (args.length >= 1) {
            // Si hay argumentos, es modo prueba de carga
            modoAutomatico(args);
        } else {
            // Sin argumentos, modo interactivo
            modoInteractivo();
        }
    }
    
    /**
     * Modo interactivo del cliente
     */
    private static void modoInteractivo() {
        Scanner scanner = new Scanner(System.in);
        boolean continuar = true;
        
        log("Cliente bancario iniciado en modo interactivo");
        
        // Configurar IP del servidor
        System.out.print("Ingrese la IP del servidor (o presione Enter para usar localhost): ");
        String ipIngresada = scanner.nextLine().trim();
        if (!ipIngresada.isEmpty()) {
            IP_SERVIDOR = ipIngresada;
        }
        log("Usando servidor: " + IP_SERVIDOR + ":" + PUERTO_SERVIDOR);
        
        while (continuar) {
            mostrarMenu();
            
            try {
                int opcion = scanner.nextInt();
                scanner.nextLine(); // Consumir el salto de línea
                
                switch (opcion) {
                    case 1: // Consultar saldo
                        System.out.print("Ingrese el ID de la cuenta: ");
                        int idCuenta = scanner.nextInt();
                        scanner.nextLine(); // Consumir el salto de línea
                        
                        String resultadoConsulta = consultarSaldo(idCuenta);
                        System.out.println("Resultado: " + resultadoConsulta);
                        break;
                        
                    case 2: // Transferir fondos
                        System.out.print("Ingrese cuenta origen: ");
                        int cuentaOrigen = scanner.nextInt();
                        scanner.nextLine();
                        
                        System.out.print("Ingrese cuenta destino: ");
                        int cuentaDestino = scanner.nextInt();
                        scanner.nextLine();
                        
                        System.out.print("Ingrese monto a transferir: ");
                        double monto = scanner.nextDouble();
                        scanner.nextLine();
                        
                        String resultadoTransferencia = transferirFondos(cuentaOrigen, cuentaDestino, monto);
                        System.out.println("Resultado: " + resultadoTransferencia);
                        break;
                        
                    case 3: // Prueba de carga
                        System.out.print("Ingrese ID del cliente: ");
                        int idCliente = scanner.nextInt();
                        scanner.nextLine();
                        
                        System.out.print("Ingrese número de transacciones: ");
                        int numTransacciones = scanner.nextInt();
                        scanner.nextLine();
                        
                        realizarPruebaCarga(idCliente, numTransacciones);
                        break;
                        
                    case 0: // Salir
                        continuar = false;
                        System.out.println("Saliendo del cliente bancario...");
                        break;
                        
                    default:
                        System.out.println("Opción no válida. Intente de nuevo.");
                }
                
            } catch (Exception e) {
                System.err.println("Error: " + e.getMessage());
                scanner.nextLine(); // Limpiar el buffer
            }
            
            // Pausa para que el usuario pueda leer el resultado
            if (continuar) {
                System.out.println("\nPresione Enter para continuar...");
                scanner.nextLine();
            }
        }
        
        scanner.close();
    }
    
    /**
     * Muestra el menú de opciones
     */
    private static void mostrarMenu() {
        System.out.println("\n===== CLIENTE BANCARIO =====");
        System.out.println("1. Consultar saldo");
        System.out.println("2. Transferir fondos");
        System.out.println("3. Realizar prueba de carga");
        System.out.println("0. Salir");
        System.out.print("Ingrese una opción: ");
    }
    
    /**
     * Modo automático para pruebas de carga
     */
    private static void modoAutomatico(String[] args) {
        try {
            int idCliente = Integer.parseInt(args[0]);
            int numTransacciones = args.length > 1 ? Integer.parseInt(args[1]) : 10;
            
            // Si hay un tercer argumento, es la IP del servidor
            if (args.length > 2) {
                IP_SERVIDOR = args[2];
            }
            
            log("Cliente " + idCliente + " iniciado en modo automático");
            log("Servidor: " + IP_SERVIDOR + ":" + PUERTO_SERVIDOR);
            log("Realizando " + numTransacciones + " transacciones");
            
            realizarPruebaCarga(idCliente, numTransacciones);
            
        } catch (NumberFormatException e) {
            System.err.println("Error en los argumentos: " + e.getMessage());
            System.err.println("Uso: java Cliente <id_cliente> [num_transacciones] [ip_servidor]");
        }
    }
    
    /**
     * Realiza una prueba de carga con múltiples transacciones
     */
    private static void realizarPruebaCarga(int idCliente, int numTransacciones) {
        log("Iniciando prueba de carga: " + numTransacciones + " transacciones");
        
        // Crear pool de hilos para las transacciones
        ExecutorService executor = Executors.newFixedThreadPool(10);
        
        // Contador para transacciones completadas
        final int[] completadas = {0};
        final int[] errores = {0};
        
        // Tiempo de inicio
        long tiempoInicio = System.currentTimeMillis();
        
        // Enviar transacciones
        for (int i = 0; i < numTransacciones; i++) {
            final int numTrans = i;
            executor.submit(() -> {
                try {
                    // Delay aleatorio para simular concurrencia real
                    Thread.sleep(random.nextInt(1000) + 100);
                    
                    // Alternamos entre consultar saldo y transferir fondos
                    if (numTrans % 2 == 0) {
                        int idCuenta = idCliente * 100 + random.nextInt(10);
                        String resultado = consultarSaldo(idCuenta);
                        log("Consulta saldo cuenta " + idCuenta + ": " + resultado);
                    } else {
                        int cuentaOrigen = idCliente * 100 + random.nextInt(10);
                        int cuentaDestino = ((idCliente + 1) % 10) * 100 + random.nextInt(10);
                        double monto = random.nextDouble() * 100;
                        String resultado = transferirFondos(cuentaOrigen, cuentaDestino, monto);
                        log("Transferencia de " + monto + " desde " + cuentaOrigen + 
                             " a " + cuentaDestino + ": " + resultado);
                    }
                    
                    // Incrementar contador de completadas
                    synchronized (completadas) {
                        completadas[0]++;
                    }
                } catch (Exception e) {
                    log("Error en transacción " + numTrans + ": " + e.getMessage());
                    synchronized (errores) {
                        errores[0]++;
                    }
                }
            });
        }
        
        // Esperar a que terminen todas las transacciones o timeout
        try {
            executor.shutdown();
            boolean terminado = executor.awaitTermination(5, TimeUnit.MINUTES);
            
            // Calcular tiempo total
            long tiempoFinal = System.currentTimeMillis();
            double segundosTranscurridos = (tiempoFinal - tiempoInicio) / 1000.0;
            
            // Mostrar resultados
            log("Prueba de carga finalizada");
            log("Transacciones completadas: " + completadas[0] + "/" + numTransacciones);
            log("Errores: " + errores[0]);
            log("Tiempo total: " + segundosTranscurridos + " segundos");
            log("Transacciones por segundo: " + (completadas[0] / segundosTranscurridos));
            
            System.out.println("\n===== RESULTADO PRUEBA DE CARGA =====");
            System.out.println("Transacciones completadas: " + completadas[0] + "/" + numTransacciones);
            System.out.println("Errores: " + errores[0]);
            System.out.println("Tiempo total: " + segundosTranscurridos + " segundos");
            System.out.println("Transacciones por segundo: " + (completadas[0] / segundosTranscurridos));
            
            if (!terminado) {
                log("La prueba de carga alcanzó el timeout");
                System.out.println("ADVERTENCIA: Algunas transacciones no se completaron (timeout)");
            }
        } catch (InterruptedException e) {
            log("Prueba interrumpida: " + e.getMessage());
            System.err.println("Prueba interrumpida: " + e.getMessage());
            Thread.currentThread().interrupt();
        }
    }
    
    /**
     * Consulta el saldo de una cuenta
     */
    private static String consultarSaldo(int idCuenta) throws IOException {
        try (Socket socket = new Socket(IP_SERVIDOR, PUERTO_SERVIDOR);
             PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
             BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {
            
            // Formato: REQUEST|CONSULTAR_SALDO|ID_CUENTA
            String request = "REQUEST|CONSULTAR_SALDO|" + idCuenta;
            out.println(request);
            
            // Leer respuesta
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
            
            // Formato: REQUEST|TRANSFERIR_FONDOS|CUENTA_ORIGEN|CUENTA_DESTINO|MONTO
            String request = String.format("REQUEST|TRANSFERIR_FONDOS|%d|%d|%.2f", 
                                        cuentaOrigen, cuentaDestino, monto);
            out.println(request);
            
            // Leer respuesta
            return in.readLine();
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
            // Asegurar que el directorio existe
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
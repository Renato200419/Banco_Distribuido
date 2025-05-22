import * as net from 'net';
import * as fs from 'fs';
import * as path from 'path';

/**
 * NodoTrabajador - Implementación TypeScript de un nodo trabajador
 * 
 * CORREGIDO PARA FUNCIONAR CON EL SERVIDOR CENTRAL
 * - Carga datos desde particiones correctas
 * - Maneja miles de cuentas
 * - Compatible con IDs 101-5100
 */
class NodoTrabajador {
    // Configuración del nodo
    private ID_NODO: number = 2; // Nodo TypeScript por defecto
    private PUERTO: number = 9102;
    private IP_SERVIDOR_CENTRAL: string = '192.168.18.31';
    private PUERTO_SERVIDOR_CENTRAL: number = 9000;
    
    // Directorio de datos
    private readonly DATA_DIR: string = '../data';
    
    // Conjunto de particiones que gestiona este nodo
    private particiones: Set<string> = new Set<string>();
    
    // Para registro de actividad
    private readonly LOG_FILE: string;
    
    // Estructuras de datos para clientes y cuentas
    private clientes: Map<number, Map<string, string>> = new Map();
    private cuentas: Map<number, Map<string, any>> = new Map();
    private transacciones: Array<Map<string, any>> = [];
    
    // Locks para operaciones concurrentes (simulados)
    private cuentaLocks: Map<number, boolean> = new Map();
    private transaccionLock: boolean = false;
    
    // Servidor
    private server!: net.Server;
    
    /**
     * Constructor
     */
    constructor(idNodo?: number, ipServidorCentral?: string) {
        if (idNodo) {
            this.ID_NODO = idNodo;
            this.PUERTO = 9100 + this.ID_NODO;
        }
        
        if (ipServidorCentral) {
            this.IP_SERVIDOR_CENTRAL = ipServidorCentral;
        }
        
        this.LOG_FILE = path.join('../logs', `nodo${this.ID_NODO}.log`);
        
        // Inicializar el nodo
        this.inicializar();
    }
    
    /**
     * Inicializa el nodo trabajador
     */
    private inicializar(): void {
        this.log('=== NODO TRABAJADOR TYPESCRIPT ===');
        this.log(`ID Nodo: ${this.ID_NODO}, Puerto: ${this.PUERTO}`);
        this.log(`Servidor Central: ${this.IP_SERVIDOR_CENTRAL}:${this.PUERTO_SERVIDOR_CENTRAL}`);
        
        // Crear directorios necesarios
        this.crearDirectorios();
        
        // Configurar particiones según el ID del nodo
        this.configurarParticiones();
        
        // Cargar datos
        this.cargarDatos();
        
        // Iniciar el servidor
        this.iniciarServidor();
        
        this.log('Nodo trabajador inicializado correctamente');
    }
    
    /**
     * Configura las particiones que gestionará este nodo
     */
    private configurarParticiones(): void {
        // Configuración según ID del nodo (compatible con ServidorCentral)
        switch (this.ID_NODO) {
            case 1:
                this.particiones.add('parte1');
                this.particiones.add('parte2');
                this.particiones.add('parte3');
                break;
                
            case 2:
                this.particiones.add('parte1');
                this.particiones.add('parte2');
                this.particiones.add('parte4');
                break;
                
            case 3:
                this.particiones.add('parte2');
                this.particiones.add('parte3');
                this.particiones.add('parte4');
                break;
                
            case 4:
                this.particiones.add('parte1');
                this.particiones.add('parte3');
                this.particiones.add('parte4');
                break;
                
            default:
                this.particiones.add('parte1');
                this.particiones.add('parte2');
                break;
        }
        
        this.log(`Particiones configuradas: ${Array.from(this.particiones).join(', ')}`);
    }
    
    /**
     * Carga los datos de clientes y cuentas
     */
    private cargarDatos(): void {
        this.log('Iniciando carga de datos...');
        
        // Cargar clientes
        this.cargarClientes();
        
        // Cargar cuentas desde particiones
        this.cargarCuentasDesdeParticiones();
        
        // Cargar transacciones
        this.cargarTransacciones();
        
        this.log('Datos cargados correctamente:');
        this.log(`  - Clientes: ${this.clientes.size}`);
        this.log(`  - Cuentas: ${this.cuentas.size}`);
        this.log(`  - Transacciones: ${this.transacciones.length}`);
    }
    
    /**
     * Crea los directorios necesarios
     */
    private crearDirectorios(): void {
        try {
            // Crear directorio de logs
            const logsDir = path.dirname(this.LOG_FILE);
            if (!fs.existsSync(logsDir)) {
                fs.mkdirSync(logsDir, { recursive: true });
            }
            
            // Crear directorios de datos si no existen
            if (!fs.existsSync(this.DATA_DIR)) {
                fs.mkdirSync(this.DATA_DIR, { recursive: true });
            }
            
            const clientesDir = path.join(this.DATA_DIR, 'clientes');
            if (!fs.existsSync(clientesDir)) {
                fs.mkdirSync(clientesDir, { recursive: true });
            }
            
            const transaccionesDir = path.join(this.DATA_DIR, 'transacciones');
            if (!fs.existsSync(transaccionesDir)) {
                fs.mkdirSync(transaccionesDir, { recursive: true });
            }
            
            // Crear directorios de particiones
            for (let i = 1; i <= 4; i++) {
                const particionDir = path.join(this.DATA_DIR, `parte${i}`);
                if (!fs.existsSync(particionDir)) {
                    fs.mkdirSync(particionDir, { recursive: true });
                }
            }
            
            this.log('Directorios creados correctamente');
        } catch (err) {
            this.log(`Error creando directorios: ${err}`);
        }
    }
    
    /**
     * Carga los datos de clientes desde archivos
     */
    private cargarClientes(): void {
        const clientesFile = path.join(this.DATA_DIR, 'clientes', 'clientes.txt');
        
        if (!fs.existsSync(clientesFile)) {
            this.log('Archivo de clientes no encontrado, esperando que el servidor lo cree...');
            return;
        }
        
        try {
            const contenido = fs.readFileSync(clientesFile, 'utf8');
            const lineas = contenido.split('\n');
            
            for (const linea of lineas) {
                if (linea.trim() === '') continue;
                
                const partes = linea.split('|');
                if (partes.length >= 4) {
                    const idCliente = parseInt(partes[0]);
                    const cliente = new Map<string, string>();
                    cliente.set('nombre', partes[1]);
                    cliente.set('email', partes[2]);
                    cliente.set('telefono', partes[3]);
                    this.clientes.set(idCliente, cliente);
                }
            }
            
            this.log(`Clientes cargados: ${this.clientes.size}`);
        } catch (err) {
            this.log(`Error cargando clientes: ${err}`);
        }
    }
    
    /**
     * Carga cuentas desde las particiones configuradas
     */
    private cargarCuentasDesdeParticiones(): void {
        for (const particion of this.particiones) {
            const particionFile = path.join(this.DATA_DIR, particion, `cuentas_${particion}.txt`);
            
            if (!fs.existsSync(particionFile)) {
                this.log(`Archivo de partición no encontrado: ${particionFile}`);
                continue;
            }
            
            try {
                const contenido = fs.readFileSync(particionFile, 'utf8');
                const lineas = contenido.split('\n');
                let cuentasCargadas = 0;
                
                for (const linea of lineas) {
                    if (linea.trim() === '') continue;
                    
                    const partes = linea.split('|');
                    if (partes.length >= 4) {
                        const idCuenta = parseInt(partes[0]);
                        const cuenta = new Map<string, any>();
                        cuenta.set('id_cliente', parseInt(partes[1]));
                        cuenta.set('saldo', parseFloat(partes[2]));
                        cuenta.set('tipo_cuenta', partes[3]);
                        this.cuentas.set(idCuenta, cuenta);
                        
                        // Crear lock para esta cuenta
                        this.cuentaLocks.set(idCuenta, false);
                        cuentasCargadas++;
                    }
                }
                
                this.log(`Partición ${particion}: ${cuentasCargadas} cuentas cargadas`);
            } catch (err) {
                this.log(`Error cargando partición ${particion}: ${err}`);
            }
        }
    }
    
    /**
     * Carga las transacciones desde archivos
     */
    private cargarTransacciones(): void {
        const transaccionesFile = path.join(this.DATA_DIR, 'transacciones', 'transacciones.txt');
        
        if (!fs.existsSync(transaccionesFile)) {
            this.log('Archivo de transacciones no encontrado, creando archivo vacío...');
            try {
                fs.writeFileSync(transaccionesFile, '');
            } catch (err) {
                this.log(`Error creando archivo de transacciones: ${err}`);
            }
            return;
        }
        
        try {
            const contenido = fs.readFileSync(transaccionesFile, 'utf8');
            const lineas = contenido.split('\n');
            
            for (const linea of lineas) {
                if (linea.trim() === '') continue;
                
                const partes = linea.split('|');
                if (partes.length >= 6) {
                    const transaccion = new Map<string, any>();
                    transaccion.set('id_transacc', parseInt(partes[0]));
                    transaccion.set('id_orig', parseInt(partes[1]));
                    transaccion.set('id_dest', parseInt(partes[2]));
                    transaccion.set('monto', parseFloat(partes[3]));
                    transaccion.set('fecha_hora', partes[4]);
                    transaccion.set('estado', partes[5]);
                    this.transacciones.push(transaccion);
                }
            }
        } catch (err) {
            this.log(`Error cargando transacciones: ${err}`);
        }
    }
    
    /**
     * Inicia el servidor para escuchar solicitudes
     */
    private iniciarServidor(): void {
        this.log(`Iniciando servidor en puerto ${this.PUERTO}`);
        
        this.server = net.createServer((socket) => {
            this.log(`Nueva conexión desde ${socket.remoteAddress}:${socket.remotePort}`);
            
            // Buffer para acumular datos
            let buffer = '';
            
            // Evento de recepción de datos
            socket.on('data', (data) => {
                buffer += data.toString();
                
                // Procesar líneas completas
                const lines = buffer.split('\n');
                buffer = lines.pop() || ''; // Guardar la última línea incompleta
                
                for (const line of lines) {
                    if (line.trim() === '') continue;
                    this.manejarSolicitud(line.trim(), socket);
                }
            });
            
            // Evento de cierre
            socket.on('close', () => {
                this.log(`Conexión cerrada desde ${socket.remoteAddress}:${socket.remotePort}`);
            });
            
            // Evento de error
            socket.on('error', (err) => {
                this.log(`Error en conexión: ${err.message}`);
            });
        });
        
        // Manejar errores del servidor
        this.server.on('error', (err) => {
            this.log(`Error en el servidor: ${err.message}`);
        });
        
        // Iniciar el servidor
        this.server.listen(this.PUERTO, () => {
            this.log(`Servidor listo para recibir conexiones en puerto ${this.PUERTO}`);
        });
    }
    
    /**
     * Maneja una solicitud entrante
     */
    private manejarSolicitud(solicitud: string, socket: net.Socket): void {
        if (!solicitud) {
            // Health check o conexión cerrada
            return;
        }
        
        this.log(`Solicitud recibida: ${solicitud}`);
        
        try {
            // Parsear la solicitud: TASK|idTarea|operacion|param1|param2|...
            const partes = solicitud.split('|');
            
            if (partes.length < 3 || partes[0] !== 'TASK') {
                socket.write('ERROR|Formato de solicitud inválido\n');
                return;
            }
            
            const idTarea = parseInt(partes[1]);
            const operacion = partes[2];
            const parametros = partes.slice(3);
            
            // Procesar según la operación
            let resultado: string;
            
            switch (operacion) {
                case 'CONSULTAR_SALDO':
                    resultado = this.consultarSaldo(parametros);
                    break;
                    
                case 'TRANSFERIR_FONDOS':
                    resultado = this.transferirFondos(parametros);
                    break;
                    
                default:
                    resultado = `ERROR|Operación no soportada: ${operacion}`;
                    break;
            }
            
            // Enviar respuesta: RESPONSE|idTarea|resultado
            const respuesta = `RESPONSE|${idTarea}|${resultado}\n`;
            socket.write(respuesta);
            this.log(`Respuesta enviada para tarea ${idTarea}: ${resultado}`);
            
        } catch (err) {
            this.log(`Error procesando solicitud: ${err}`);
            socket.write(`ERROR|${err}\n`);
        }
    }
    
    /**
     * Consulta el saldo de una cuenta
     */
    private consultarSaldo(parametros: string[]): string {
        if (parametros.length < 1) {
            return 'ERROR|Faltan parámetros para consultar saldo';
        }
        
        try {
            const idCuenta = parseInt(parametros[0]);
            
            // Verificar si tenemos la cuenta
            if (!this.cuentas.has(idCuenta)) {
                return `ERROR|Cuenta no encontrada: ${idCuenta}`;
            }
            
            // Obtener el saldo con lock
            if (!this.adquirirLockCuenta(idCuenta)) {
                return 'ERROR|No se pudo adquirir lock para la cuenta';
            }
            
            try {
                const saldo = this.cuentas.get(idCuenta)!.get('saldo');
                return `OK|${saldo.toFixed(2)}`;
            } finally {
                this.liberarLockCuenta(idCuenta);
            }
            
        } catch (err) {
            this.log(`Error consultando saldo: ${err}`);
            return `ERROR|${err}`;
        }
    }
    
    /**
     * Realiza una transferencia entre cuentas
     */
    private transferirFondos(parametros: string[]): string {
        if (parametros.length < 3) {
            return 'ERROR|Faltan parámetros para transferencia';
        }
        
        try {
            const cuentaOrigen = parseInt(parametros[0]);
            const cuentaDestino = parseInt(parametros[1]);
            const monto = parseFloat(parametros[2]);
            
            // Validaciones básicas
            if (monto <= 0) {
                return 'ERROR|El monto debe ser positivo';
            }
            
            if (!this.cuentas.has(cuentaOrigen)) {
                return `ERROR|Cuenta origen no encontrada: ${cuentaOrigen}`;
            }
            
            if (!this.cuentas.has(cuentaDestino)) {
                return `ERROR|Cuenta destino no encontrada: ${cuentaDestino}`;
            }
            
            // Adquirir locks en orden para evitar deadlocks
            const locks = this.adquirirLocksOrdenados(cuentaOrigen, cuentaDestino);
            if (!locks) {
                return 'ERROR|No se pudieron adquirir locks para las cuentas';
            }
            
            try {
                // Verificar saldo suficiente
                const saldoOrigen = this.cuentas.get(cuentaOrigen)!.get('saldo');
                
                if (saldoOrigen < monto) {
                    return `ERROR|Saldo insuficiente. Disponible: ${saldoOrigen.toFixed(2)}`;
                }
                
                // Realizar la transferencia
                this.cuentas.get(cuentaOrigen)!.set('saldo', saldoOrigen - monto);
                
                const saldoDestino = this.cuentas.get(cuentaDestino)!.get('saldo');
                this.cuentas.get(cuentaDestino)!.set('saldo', saldoDestino + monto);
                
                // Registrar la transacción
                this.registrarTransaccion(cuentaOrigen, cuentaDestino, monto);
                
                return 'OK|Transferencia completada';
                
            } finally {
                this.liberarLockCuenta(cuentaOrigen);
                this.liberarLockCuenta(cuentaDestino);
            }
            
        } catch (err) {
            this.log(`Error en transferencia: ${err}`);
            return `ERROR|${err}`;
        }
    }
    
    /**
     * Registra una transacción
     */
    private registrarTransaccion(cuentaOrigen: number, cuentaDestino: number, monto: number): void {
        if (!this.adquirirLockTransaccion()) {
            this.log('No se pudo registrar la transacción - lock no disponible');
            return;
        }
        
        try {
            const idTransaccion = this.transacciones.length + 1;
            
            // Fecha actual formateada
            const now = new Date();
            const fechaHora = `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, '0')}-${String(now.getDate()).padStart(2, '0')} ${String(now.getHours()).padStart(2, '0')}:${String(now.getMinutes()).padStart(2, '0')}:${String(now.getSeconds()).padStart(2, '0')}`;
            
            // Crear registro de transacción
            const transaccion = new Map<string, any>();
            transaccion.set('id_transacc', idTransaccion);
            transaccion.set('id_orig', cuentaOrigen);
            transaccion.set('id_dest', cuentaDestino);
            transaccion.set('monto', monto);
            transaccion.set('fecha_hora', fechaHora);
            transaccion.set('estado', 'Confirmada');
            
            this.transacciones.push(transaccion);
            
            this.log(`Transacción registrada: ${cuentaOrigen} -> ${cuentaDestino} (${monto})`);
            
        } finally {
            this.liberarLockTransaccion();
        }
    }
    
    /**
     * Adquiere un lock para una cuenta
     */
    private adquirirLockCuenta(idCuenta: number): boolean {
        let intentos = 0;
        const maxIntentos = 50;
        
        while (intentos < maxIntentos) {
            if (!this.cuentaLocks.get(idCuenta)) {
                this.cuentaLocks.set(idCuenta, true);
                return true;
            }
            
            // Esperar un poco antes de reintentar
            this.sleep(100);
            intentos++;
        }
        
        return false;
    }
    
    /**
     * Libera un lock para una cuenta
     */
    private liberarLockCuenta(idCuenta: number): void {
        this.cuentaLocks.set(idCuenta, false);
    }
    
    /**
     * Adquiere locks para dos cuentas en orden para evitar deadlocks
     */
    private adquirirLocksOrdenados(cuenta1: number, cuenta2: number): boolean {
        const [primera, segunda] = cuenta1 < cuenta2 ? [cuenta1, cuenta2] : [cuenta2, cuenta1];
        
        if (!this.adquirirLockCuenta(primera)) {
            return false;
        }
        
        if (!this.adquirirLockCuenta(segunda)) {
            this.liberarLockCuenta(primera);
            return false;
        }
        
        return true;
    }
    
    /**
     * Adquiere un lock para transacciones
     */
    private adquirirLockTransaccion(): boolean {
        let intentos = 0;
        const maxIntentos = 50;
        
        while (intentos < maxIntentos) {
            if (!this.transaccionLock) {
                this.transaccionLock = true;
                return true;
            }
            
            this.sleep(100);
            intentos++;
        }
        
        return false;
    }
    
    /**
     * Libera el lock para transacciones
     */
    private liberarLockTransaccion(): void {
        this.transaccionLock = false;
    }
    
    /**
     * Función de sleep
     */
    private sleep(ms: number): void {
        const start = Date.now();
        while (Date.now() - start < ms) {
            // Esperar
        }
    }
    
    /**
     * Registra un mensaje de log
     */
    private log(mensaje: string): void {
        const now = new Date();
        const timestamp = `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, '0')}-${String(now.getDate()).padStart(2, '0')} ${String(now.getHours()).padStart(2, '0')}:${String(now.getMinutes()).padStart(2, '0')}:${String(now.getSeconds()).padStart(2, '0')}`;
        const logLine = `[${timestamp}] ${mensaje}`;
        
        // Imprimir en consola
        console.log(logLine);
        
        // Escribir en archivo de log
        try {
            fs.appendFileSync(this.LOG_FILE, logLine + '\n');
        } catch (err) {
            console.error(`Error escribiendo en log: ${err}`);
        }
    }
    
    /**
     * Cierra el servidor
     */
    public cerrar(): void {
        if (this.server) {
            this.server.close();
            this.log('Servidor cerrado');
        }
    }
}

// Punto de entrada
function main(): void {
    // Obtener argumentos de la línea de comandos
    const args = process.argv.slice(2);
    
    let idNodo: number | undefined;
    let ipServidorCentral: string | undefined;
    
    if (args.length >= 1) {
        idNodo = parseInt(args[0]);
    }
    
    if (args.length >= 2) {
        ipServidorCentral = args[1];
    }
    
    // Crear y ejecutar el nodo trabajador
    const nodo = new NodoTrabajador(idNodo, ipServidorCentral);
    
    // Manejar señales para cierre adecuado
    process.on('SIGINT', () => {
        console.log('\nCerrando el nodo trabajador...');
        nodo.cerrar();
        process.exit(0);
    });
    
    process.on('SIGTERM', () => {
        console.log('\nCerrando el nodo trabajador...');
        nodo.cerrar();
        process.exit(0);
    });
}

// Ejecutar el programa
main();

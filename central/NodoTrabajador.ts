import * as net from 'net';
import * as fs from 'fs';
import * as path from 'path';
import * as readline from 'readline';

/**
 * NodoTrabajador - Implementación TypeScript de un nodo trabajador
 * 
 * Este nodo procesará solicitudes del Servidor Central para consultar saldos 
 * y realizar transferencias entre cuentas. Gestiona particiones de datos
 * y soporta replicación.
 */
class NodoTrabajador2 {
    // Configuración del nodo
    private ID_NODO: number = 3; // Por defecto, el nodo 3 es TypeScript según el código Java
    private PUERTO: number = 9103; // 9100 + ID_NODO
    private IP_SERVIDOR_CENTRAL: string = '192.168.18.29'; // IP de la VM donde está el servidor central
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
    
    // Locks para operaciones concurrentes (simulados con Map en TypeScript)
    private cuentaLocks: Map<number, boolean> = new Map();
    private transaccionLock: boolean = false;
    
    // Servidor
    private server!: net.Server; // Usando el operador ! para indicar que será inicializado
    
    /**
     * Constructor
     */
    constructor(idNodo?: number, ipServidorCentral?: string) {
        // Procesar argumentos si se proporcionan
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
        this.log(`Iniciando Nodo Trabajador ${this.ID_NODO} en puerto ${this.PUERTO}`);
        
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
        // Esto dependerá del ID del nodo y debe coincidir con la configuración
        // del servidor central
        switch (this.ID_NODO) {
            case 1:
                this.particiones.add('parte1.1');
                this.particiones.add('parte2.1');
                this.particiones.add('parte2.2');
                this.particiones.add('parte2.3');
                break;
                
            case 2:
                this.particiones.add('parte1.1');
                this.particiones.add('parte1.2');
                this.particiones.add('parte2.2');
                this.particiones.add('parte2.3');
                this.particiones.add('parte2.4');
                break;
                
            case 3:
                this.particiones.add('parte1.1');
                this.particiones.add('parte1.2');
                this.particiones.add('parte1.3');
                this.particiones.add('parte2.3');
                this.particiones.add('parte2.4');
                break;
                
            default:
                // Configuración genérica para otros nodos
                this.particiones.add('parte1.1');
                this.particiones.add('parte2.1');
                break;
        }
        
        this.log(`Particiones configuradas: ${Array.from(this.particiones).join(', ')}`);
    }
    
    /**
     * Carga los datos de clientes y cuentas
     */
    private cargarDatos(): void {
        // Crear directorios si no existen
        this.crearDirectorios();
        
        // Cargar clientes
        this.log('Cargando datos de clientes...');
        this.cargarClientes();
        
        // Cargar cuentas
        this.log('Cargando datos de cuentas...');
        this.cargarCuentas();
        
        // Cargar transacciones
        this.log('Cargando datos de transacciones...');
        this.cargarTransacciones();
        
        this.log(`Datos cargados correctamente. Clientes: ${this.clientes.size}, Cuentas: ${this.cuentas.size}, Transacciones: ${this.transacciones.length}`);
    }
    
    /**
     * Crea los directorios necesarios
     */
    private crearDirectorios(): void {
        try {
            // Directorios para datos
            if (!fs.existsSync(this.DATA_DIR)) {
                fs.mkdirSync(this.DATA_DIR, { recursive: true });
            }
            
            const clientesDir = path.join(this.DATA_DIR, 'clientes');
            if (!fs.existsSync(clientesDir)) {
                fs.mkdirSync(clientesDir, { recursive: true });
            }
            
            const cuentasDir = path.join(this.DATA_DIR, 'cuentas');
            if (!fs.existsSync(cuentasDir)) {
                fs.mkdirSync(cuentasDir, { recursive: true });
            }
            
            const transaccionesDir = path.join(this.DATA_DIR, 'transacciones');
            if (!fs.existsSync(transaccionesDir)) {
                fs.mkdirSync(transaccionesDir, { recursive: true });
            }
            
            // Directorio para logs
            const logsDir = path.dirname(this.LOG_FILE);
            if (!fs.existsSync(logsDir)) {
                fs.mkdirSync(logsDir, { recursive: true });
            }
        } catch (err) {
            console.error('Error creando directorios:', err);
        }
    }
    
    /**
     * Carga los datos de clientes desde archivos
     */
    private cargarClientes(): void {
        const clientesFile = path.join(this.DATA_DIR, 'clientes', 'clientes.txt');
        
        if (!fs.existsSync(clientesFile)) {
            this.log('Archivo de clientes no encontrado. Creando archivo de prueba...');
            fs.writeFileSync(clientesFile, 
                '1|Juan Pérez|juan@email.com|987654321\n' +
                '2|María López|maria@email.com|998877665\n' +
                '3|Carlos Rodríguez|carlos@email.com|912345678\n'
            );
        }
        
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
    }
    
    /**
     * Carga los datos de cuentas desde archivos
     */
    private cargarCuentas(): void {
        const cuentasFile = path.join(this.DATA_DIR, 'cuentas', 'cuentas.txt');
        
        if (!fs.existsSync(cuentasFile)) {
            this.log('Archivo de cuentas no encontrado. Creando archivo de prueba...');
            fs.writeFileSync(cuentasFile, 
                '101|1|1500.00|Ahorros\n' +
                '102|2|3200.50|Corriente\n' +
                '103|3|2100.75|Ahorros\n'
            );
        }
        
        const contenido = fs.readFileSync(cuentasFile, 'utf8');
        const lineas = contenido.split('\n');
        
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
            }
        }
    }
    
    /**
     * Carga las transacciones desde archivos
     */
    private cargarTransacciones(): void {
        const transaccionesFile = path.join(this.DATA_DIR, 'transacciones', 'transacciones.txt');
        
        if (!fs.existsSync(transaccionesFile)) {
            this.log('Archivo de transacciones no encontrado. Creando archivo vacío...');
            fs.writeFileSync(transaccionesFile, '');
        } else {
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
        }
    }
    
    /**
     * Guarda las transacciones en el archivo
     */
    private guardarTransacciones(): void {
        try {
            const transaccionesFile = path.join(this.DATA_DIR, 'transacciones', 'transacciones.txt');
            let contenido = '';
            
            for (const transaccion of this.transacciones) {
                contenido += `${transaccion.get('id_transacc')}|` +
                             `${transaccion.get('id_orig')}|` +
                             `${transaccion.get('id_dest')}|` +
                             `${transaccion.get('monto')}|` +
                             `${transaccion.get('fecha_hora')}|` +
                             `${transaccion.get('estado')}\n`;
            }
            
            fs.writeFileSync(transaccionesFile, contenido);
        } catch (err) {
            this.log(`Error guardando transacciones: ${err}`);
        }
    }
    
    /**
     * Guarda las cuentas en el archivo
     */
    private guardarCuentas(): void {
        try {
            const cuentasFile = path.join(this.DATA_DIR, 'cuentas', 'cuentas.txt');
            let contenido = '';
            
            for (const [idCuenta, cuenta] of this.cuentas.entries()) {
                contenido += `${idCuenta}|` +
                             `${cuenta.get('id_cliente')}|` +
                             `${cuenta.get('saldo')}|` +
                             `${cuenta.get('tipo_cuenta')}\n`;
            }
            
            fs.writeFileSync(cuentasFile, contenido);
        } catch (err) {
            this.log(`Error guardando cuentas: ${err}`);
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
            this.log(`Servidor escuchando en puerto ${this.PUERTO}`);
        });
    }
    
    /**
     * Maneja una solicitud entrante
     */
    private manejarSolicitud(solicitud: string, socket: net.Socket): void {
        this.log(`Solicitud recibida: ${solicitud}`);
        
        try {
            // Parsear la solicitud
            // Formato esperado: TASK|idTarea|operacion|param1|param2|...
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
            
            // Enviar respuesta
            // Formato: RESPONSE|idTarea|resultado
            const respuesta = `RESPONSE|${idTarea}|${resultado}\n`;
            socket.write(respuesta);
            this.log(`Respuesta enviada: ${respuesta}`);
            
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
            
            // Obtener el saldo (con lock para evitar condiciones de carrera)
            const lock = this.adquirirLockCuenta(idCuenta);
            if (!lock) {
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
                    return 'ERROR|Saldo insuficiente';
                }
                
                // Realizar la transferencia
                this.cuentas.get(cuentaOrigen)!.set('saldo', saldoOrigen - monto);
                
                const saldoDestino = this.cuentas.get(cuentaDestino)!.get('saldo');
                this.cuentas.get(cuentaDestino)!.set('saldo', saldoDestino + monto);
                
                // Registrar la transacción
                if (!this.adquirirLockTransaccion()) {
                    return 'ERROR|No se pudo adquirir lock para registrar la transacción';
                }
                
                try {
                    // Generar ID de transacción (simplificado)
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
                    
                    // Guardar cambios en archivos
                    this.guardarCuentas();
                    this.guardarTransacciones();
                    
                    return 'OK|Transferencia completada';
                } finally {
                    this.liberarLockTransaccion();
                }
                
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
     * Adquiere un lock para una cuenta
     */
    private adquirirLockCuenta(idCuenta: number): boolean {
        // Implementación simple de lock con timeout
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
        // Adquirir locks en orden para evitar deadlocks
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
        // Implementación simple de lock con timeout
        let intentos = 0;
        const maxIntentos = 50;
        
        while (intentos < maxIntentos) {
            if (!this.transaccionLock) {
                this.transaccionLock = true;
                return true;
            }
            
            // Esperar un poco antes de reintentar
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
            // Error silencioso - ya imprimimos en consola
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
    const nodo = new NodoTrabajador2(idNodo, ipServidorCentral);
    
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

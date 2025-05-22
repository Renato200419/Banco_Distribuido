"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
var net = require("net");
var fs = require("fs");
var path = require("path");

/**
 * NodoTrabajador en JavaScript - COMPLETAMENTE CORREGIDO
 */
var NodoTrabajadorJs = /** @class */ (function () {
    /**
     * Constructor
     */
    function NodoTrabajadorJs(idNodo, ipServer) {
        // Configuración del nodo
        this.ID = 4; // El nodo 4 es el de tipo JavaScript
        this.PORT = 9104; // 9100 + ID
        this.IP_SERVER_CENTRAL = 'localhost'; // Apunta al server central
        
        // Directorio de datos
        this.DATA_DIR = '../data';
        
        // Conjunto de particiones que gestiona este nodo
        this.particiones = new Set();
        
        // Estructuras de datos para clientes y cuentas
        this.clientes = new Map();
        this.cuentas = new Map();
        this.transacciones = [];
        
        // Locks para operaciones concurrentes 
        this.cuentaLocks = new Map();
        this.transaccionLock = false;
        
        // Procesar argumentos si se proporcionan
        if (idNodo) {
            this.ID = idNodo;
            this.PORT = 9100 + this.ID;
        }
        if (ipServer) {
            this.IP_SERVER_CENTRAL = ipServer;
        }
        
        this.LOG_FILE = path.join('../logs', "nodo".concat(this.ID, ".log"));
        
        // Inicializar el nodo
        this.inicializar();
    }

    /**
     * Inicializa el nodo trabajador
     */
    NodoTrabajadorJs.prototype.inicializar = function () {
        this.log("Iniciando Nodo Trabajador ".concat(this.ID, " en PORT ").concat(this.PORT));
        
        // Configurar particiones según el ID del nodo
        this.configurarParticiones();
        
        // Cargar datos
        this.cargarDatos();
        
        // Iniciar el SERVER
        this.iniciarSERVER();
        
        this.log('Nodo trabajador inicializado correctamente');
    };

    /**
     * ✅ CORREGIDO: Configura las particiones que gestionará este nodo
     */
    NodoTrabajadorJs.prototype.configurarParticiones = function () {
        // Configuración según el ServerCentral
        switch (this.ID) {
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
            case 4: // ✅ ESTE NODO JAVASCRIPT
                this.particiones.add('parte1');
                this.particiones.add('parte3');
                this.particiones.add('parte4');
                break;
            default:
                this.particiones.add('parte1');
                this.particiones.add('parte2');
                break;
        }
        this.log("Particiones configuradas: ".concat(Array.from(this.particiones).join(', ')));
    };

    /**
     * ✅ CORREGIDO: Carga los datos de clientes y cuentas
     */
    NodoTrabajadorJs.prototype.cargarDatos = function () {
        // Crear directorios si no existen
        this.crearDirectorios();
        
        this.log('Cargando datos de clientes...');
        this.cargarClientes();
        
        this.log('Cargando datos de cuentas particionadas...');
        this.cargarCuentas(); // ✅ MÉTODO COMPLETAMENTE CORREGIDO
        
        this.log('Cargando datos de transacciones...');
        this.cargarTransacciones();
        
        this.log("Datos cargados correctamente. Clientes: ".concat(this.clientes.size, ", Cuentas: ").concat(this.cuentas.size, ", Transacciones: ").concat(this.transacciones.length));
    };

    /**
     * ✅ COMPLETAMENTE CORREGIDO: Carga los datos de cuentas PARTICIONADOS
     */
    NodoTrabajadorJs.prototype.cargarCuentas = function () {
        this.cuentas.clear();
        
        // 1️⃣ INTENTAR CARGAR DE ARCHIVOS PARTICIONADOS
        var cargadoDeParticiones = this.cargarDesdeParticiones();
        
        if (!cargadoDeParticiones) {
            // 2️⃣ SI NO HAY PARTICIONES, CARGAR DEL ARCHIVO CENTRAL Y FILTRAR
            this.log('⚠️  No encontré archivos particionados, cargando del archivo central...');
            this.cargarDesdeArchivoCentral();
        }
        
        this.log('✅ TOTAL CUENTAS CARGADAS: ' + this.cuentas.size);
    };

    /**
     * ✅ NUEVO: Intenta cargar de archivos particionados
     */
    NodoTrabajadorJs.prototype.cargarDesdeParticiones = function () {
        var algunaCargada = false;
        var particionesArray = Array.from(this.particiones);
        
        for (var i = 0; i < particionesArray.length; i++) {
            var particion = particionesArray[i];
            var archivoParticion = path.join(this.DATA_DIR, particion, 'cuentas_' + particion + '.txt');
            
            if (fs.existsSync(archivoParticion)) {
                this.log('📂 Cargando de: ' + archivoParticion);
                
                try {
                    var contenido = fs.readFileSync(archivoParticion, 'utf8');
                    var lineas = contenido.split('\n');
                    var cuentasEnParticion = 0;
                    
                    for (var j = 0; j < lineas.length; j++) {
                        var linea = lineas[j];
                        if (linea.trim() === '') continue;
                        
                        var partes = linea.split('|');
                        if (partes.length >= 4) {
                            var idCuenta = parseInt(partes[0]);
                            var cuenta = new Map();
                            cuenta.set('id_cliente', parseInt(partes[1]));
                            cuenta.set('saldo', parseFloat(partes[2]));
                            cuenta.set('tipo_cuenta', partes[3]);
                            
                            this.cuentas.set(idCuenta, cuenta);
                            this.cuentaLocks.set(idCuenta, false);
                            cuentasEnParticion++;
                        }
                    }
                    
                    this.log('✅ ' + particion + ': ' + cuentasEnParticion + ' cuentas');
                    algunaCargada = true;
                    
                } catch (err) {
                    this.log('❌ Error en ' + particion + ': ' + err.message);
                }
            } else {
                this.log('❌ No existe: ' + archivoParticion);
            }
        }
        
        return algunaCargada;
    };

    /**
     * ✅ NUEVO: Carga del archivo central y filtra por particiones
     */
    NodoTrabajadorJs.prototype.cargarDesdeArchivoCentral = function () {
        var archivoCentral = path.join(this.DATA_DIR, 'cuentas', 'cuentas.txt');
        
        if (fs.existsSync(archivoCentral)) {
            this.log('📂 Cargando del archivo central: ' + archivoCentral);
            
            try {
                var contenido = fs.readFileSync(archivoCentral, 'utf8');
                var lineas = contenido.split('\n');
                var cuentasFiltradas = 0;
                
                for (var i = 0; i < lineas.length; i++) {
                    var linea = lineas[i];
                    if (linea.trim() === '') continue;
                    
                    var partes = linea.split('|');
                    if (partes.length >= 4) {
                        var idCuenta = parseInt(partes[0]);
                        
                        // ✅ FILTRAR: Solo cargar cuentas de MIS particiones
                        if (this.cuentaPerteneceAMiNodo(idCuenta)) {
                            var cuenta = new Map();
                            cuenta.set('id_cliente', parseInt(partes[1]));
                            cuenta.set('saldo', parseFloat(partes[2]));
                            cuenta.set('tipo_cuenta', partes[3]);
                            
                            this.cuentas.set(idCuenta, cuenta);
                            this.cuentaLocks.set(idCuenta, false);
                            cuentasFiltradas++;
                        }
                    }
                }
                
                this.log('✅ Cuentas filtradas del central: ' + cuentasFiltradas);
                
            } catch (err) {
                this.log('❌ Error leyendo archivo central: ' + err.message);
                this.crearCuentasPrueba();
            }
        } else {
            this.log('❌ Archivo central no existe, creando datos de prueba...');
            this.crearCuentasPrueba();
        }
    };

    /**
     * ✅ NUEVO: Determina si una cuenta pertenece a las particiones de este nodo
     */
    NodoTrabajadorJs.prototype.cuentaPerteneceAMiNodo = function (idCuenta) {
        var particionCuenta = this.determinarParticionPorId(idCuenta);
        return this.particiones.has(particionCuenta);
    };

    /**
     * ✅ NUEVO: Determina la partición según el ID de cuenta
     */
    NodoTrabajadorJs.prototype.determinarParticionPorId = function (idCuenta) {
        // Distribución según rangos de ID (como en el ServerCentral)
        if (idCuenta >= 101 && idCuenta <= 1350) return 'parte1';
        if (idCuenta >= 1351 && idCuenta <= 2600) return 'parte2';
        if (idCuenta >= 2601 && idCuenta <= 3850) return 'parte3';
        if (idCuenta >= 3851 && idCuenta <= 5100) return 'parte4';
        
        // Para otros casos, usar módulo
        return 'parte' + ((idCuenta % 4) + 1);
    };

    /**
     * ✅ NUEVO: Crea cuentas de prueba para las particiones de este nodo
     */
    NodoTrabajadorJs.prototype.crearCuentasPrueba = function () {
        // Crear algunas cuentas de prueba para cada partición de este nodo
        var cuentasPrueba = [
            // parte1 (101-1350)
            [101, 1, 1500.00, 'Ahorros'],
            [102, 2, 3200.50, 'Corriente'],
            [500, 3, 2100.75, 'Ahorros'],
            [1000, 1, 800.00, 'Corriente'],
            [1200, 2, 1500.25, 'Ahorros'],
            
            // parte3 (2601-3850)
            [2601, 2, 1200.50, 'Ahorros'],
            [3000, 3, 950.25, 'Corriente'],
            [3500, 1, 1800.00, 'Ahorros'],
            [3700, 2, 2200.75, 'Corriente'],
            
            // parte4 (3851-5100)
            [4000, 2, 2200.75, 'Corriente'],
            [4500, 3, 1100.25, 'Ahorros'],
            [5000, 1, 3000.00, 'Corriente'],
            [5050, 3, 1750.50, 'Ahorros']
        ];
        
        for (var i = 0; i < cuentasPrueba.length; i++) {
            var datos = cuentasPrueba[i];
            var idCuenta = datos[0];
            
            if (this.cuentaPerteneceAMiNodo(idCuenta)) {
                var cuenta = new Map();
                cuenta.set('id_cliente', datos[1]);
                cuenta.set('saldo', datos[2]);
                cuenta.set('tipo_cuenta', datos[3]);
                
                this.cuentas.set(idCuenta, cuenta);
                this.cuentaLocks.set(idCuenta, false);
            }
        }
        
        this.log('✅ Creadas ' + this.cuentas.size + ' cuentas de prueba');
    };

    /**
     * Crea los directorios necesarios
     */
    NodoTrabajadorJs.prototype.crearDirectorios = function () {
        try {
            // Directorios para datos
            if (!fs.existsSync(this.DATA_DIR)) {
                fs.mkdirSync(this.DATA_DIR, { recursive: true });
            }
            
            var clientesDir = path.join(this.DATA_DIR, 'clientes');
            if (!fs.existsSync(clientesDir)) {
                fs.mkdirSync(clientesDir, { recursive: true });
            }
            
            var cuentasDir = path.join(this.DATA_DIR, 'cuentas');
            if (!fs.existsSync(cuentasDir)) {
                fs.mkdirSync(cuentasDir, { recursive: true });
            }
            
            var transaccionesDir = path.join(this.DATA_DIR, 'transacciones');
            if (!fs.existsSync(transaccionesDir)) {
                fs.mkdirSync(transaccionesDir, { recursive: true });
            }
            
            // Crear directorios de particiones
            var particiones = ['parte1', 'parte2', 'parte3', 'parte4'];
            for (var i = 0; i < particiones.length; i++) {
                var particionDir = path.join(this.DATA_DIR, particiones[i]);
                if (!fs.existsSync(particionDir)) {
                    fs.mkdirSync(particionDir, { recursive: true });
                }
            }
            
            // Directorio para logs
            var logsDir = path.dirname(this.LOG_FILE);
            if (!fs.existsSync(logsDir)) {
                fs.mkdirSync(logsDir, { recursive: true });
            }
        } catch (err) {
            console.error('Error creando directorios:', err);
        }
    };

    /**
     * Carga los datos de clientes desde archivos
     */
    NodoTrabajadorJs.prototype.cargarClientes = function () {
        var clientesFile = path.join(this.DATA_DIR, 'clientes', 'clientes.txt');
        
        if (fs.existsSync(clientesFile)) {
            var contenido = fs.readFileSync(clientesFile, 'utf8');
            var lineas = contenido.split('\n');
            
            for (var _i = 0, lineas_1 = lineas; _i < lineas_1.length; _i++) {
                var linea = lineas_1[_i];
                if (linea.trim() === '') continue;
                
                var partes = linea.split('|');
                if (partes.length >= 4) {
                    var idCliente = parseInt(partes[0]);
                    var cliente = new Map();
                    cliente.set('nombre', partes[1]);
                    cliente.set('email', partes[2]);
                    cliente.set('telefono', partes[3]);
                    this.clientes.set(idCliente, cliente);
                }
            }
        } else {
            this.log('Archivo de clientes no encontrado. Creando archivo de prueba...');
            fs.writeFileSync(clientesFile, '1|Juan Pérez|juan@email.com|987654321\n' +
                '2|María López|maria@email.com|998877665\n' +
                '3|Carlos Rodríguez|carlos@email.com|912345678\n');
            
            // Cargar los datos recién creados
            this.cargarClientes();
        }
    };

    /**
     * Carga las transacciones desde archivos
     */
    NodoTrabajadorJs.prototype.cargarTransacciones = function () {
        var transaccionesFile = path.join(this.DATA_DIR, 'transacciones', 'transacciones.txt');
        if (!fs.existsSync(transaccionesFile)) {
            this.log('Archivo de transacciones no encontrado. Creando archivo vacío...');
            fs.writeFileSync(transaccionesFile, '');
        } else {
            var contenido = fs.readFileSync(transaccionesFile, 'utf8');
            var lineas = contenido.split('\n');
            
            for (var _i = 0, lineas_3 = lineas; _i < lineas_3.length; _i++) {
                var linea = lineas_3[_i];
                if (linea.trim() === '') continue;
                
                var partes = linea.split('|');
                if (partes.length >= 6) {
                    var transaccion = new Map();
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
    };

    /**
     * ✅ CORREGIDO: Guarda las cuentas en archivos particionados
     */
    NodoTrabajadorJs.prototype.guardarCuentas = function () {
        try {
            // Agrupar cuentas por partición
            var cuentasPorParticion = {};
            var particionesArray = Array.from(this.particiones);
            
            for (var i = 0; i < particionesArray.length; i++) {
                cuentasPorParticion[particionesArray[i]] = [];
            }
            
            // Distribuir cuentas según su partición correspondiente
            for (var _i = 0, _a = this.cuentas.entries(); _i < _a.length; _i++) {
                var _b = _a[_i], idCuenta = _b[0], cuenta = _b[1];
                var particionDestino = this.determinarParticionPorId(idCuenta);
                
                if (cuentasPorParticion[particionDestino]) {
                    cuentasPorParticion[particionDestino].push([idCuenta, cuenta]);
                }
            }
            
            // Guardar cada partición
            for (var particion in cuentasPorParticion) {
                var cuentas = cuentasPorParticion[particion];
                if (cuentas.length > 0) {
                    var archivoParticion = path.join(this.DATA_DIR, particion, 'cuentas_' + particion + '.txt');
                    
                    var contenido = '';
                    for (var i = 0; i < cuentas.length; i++) {
                        var _c = cuentas[i], idCuenta = _c[0], cuenta = _c[1];
                        contenido += idCuenta + "|" +
                            cuenta.get('id_cliente') + "|" +
                            cuenta.get('saldo') + "|" +
                            cuenta.get('tipo_cuenta') + "\n";
                    }
                    
                    fs.writeFileSync(archivoParticion, contenido);
                    this.log('Guardadas ' + cuentas.length + ' cuentas en ' + particion);
                }
            }
        } catch (err) {
            this.log("Error guardando cuentas particionadas: " + err);
        }
    };

    /**
     * Guarda las transacciones en el archivo
     */
    NodoTrabajadorJs.prototype.guardarTransacciones = function () {
        try {
            var transaccionesFile = path.join(this.DATA_DIR, 'transacciones', 'transacciones.txt');
            var contenido = '';
            for (var _i = 0, _a = this.transacciones; _i < _a.length; _i++) {
                var transaccion = _a[_i];
                contenido += "".concat(transaccion.get('id_transacc'), "|") +
                    "".concat(transaccion.get('id_orig'), "|") +
                    "".concat(transaccion.get('id_dest'), "|") +
                    "".concat(transaccion.get('monto'), "|") +
                    "".concat(transaccion.get('fecha_hora'), "|") +
                    "".concat(transaccion.get('estado'), "\n");
            }
            fs.writeFileSync(transaccionesFile, contenido);
        } catch (err) {
            this.log("Error guardando transacciones: ".concat(err));
        }
    };

    /**
     * Inicia el SERVER para escuchar solicitudes
     */
    NodoTrabajadorJs.prototype.iniciarSERVER = function () {
        var _this = this;
        this.log("Iniciando SERVER en PORT ".concat(this.PORT));
        
        this.server = net.createServer(function (socket) {
            _this.log("Nueva conexi\u00F3n desde ".concat(socket.remoteAddress, ":").concat(socket.remotePort));
            
            var buffer = '';
            
            socket.on('data', function (data) {
                buffer += data.toString();
                var lines = buffer.split('\n');
                buffer = lines.pop() || '';
                
                for (var _i = 0, lines_1 = lines; _i < lines_1.length; _i++) {
                    var line = lines_1[_i];
                    if (line.trim() === '') continue;
                    _this.manejarSolicitud(line.trim(), socket);
                }
            });
            
            socket.on('close', function () {
                _this.log("Conexi\u00F3n cerrada desde ".concat(socket.remoteAddress, ":").concat(socket.remotePort));
            });
            
            socket.on('error', function (err) {
                _this.log("Error en conexi\u00F3n: ".concat(err.message));
            });
        });
        
        this.server.on('error', function (err) {
            _this.log("Error en el SERVER: ".concat(err.message));
        });
        
        this.server.listen(this.PORT, function () {
            _this.log("SERVER escuchando en PORT ".concat(_this.PORT));
        });
    };

    /**
     * Maneja una solicitud entrante
     */
    NodoTrabajadorJs.prototype.manejarSolicitud = function (solicitud, socket) {
        this.log("Solicitud recibida: ".concat(solicitud));
        
        try {
            var partes = solicitud.split('|');
            if (partes.length < 3 || partes[0] !== 'TASK') {
                socket.write('ERROR|Formato de solicitud inválido\n');
                return;
            }
            
            var idTarea = parseInt(partes[1]);
            var operacion = partes[2];
            var parametros = partes.slice(3);
            
            var resultado = void 0;
            switch (operacion) {
                case 'CONSULTAR_SALDO':
                    resultado = this.consultarSaldo(parametros);
                    break;
                case 'TRANSFERIR_FONDOS':
                    resultado = this.transferirFondos(parametros);
                    break;
                default:
                    resultado = "ERROR|Operaci\u00F3n no soportada: ".concat(operacion);
                    break;
            }
            
            var respuesta = "RESPONSE|".concat(idTarea, "|").concat(resultado, "\n");
            socket.write(respuesta);
            this.log("Respuesta enviada: ".concat(respuesta.trim()));
            
        } catch (err) {
            this.log("Error procesando solicitud: ".concat(err));
            socket.write("ERROR|".concat(err, "\n"));
        }
    };

    /**
     * Consulta el saldo de una cuenta
     */
    NodoTrabajadorJs.prototype.consultarSaldo = function (parametros) {
        if (parametros.length < 1) {
            return 'ERROR|Faltan parámetros para consultar saldo';
        }
        
        try {
            var idCuenta = parseInt(parametros[0]);
            
            if (!this.cuentas.has(idCuenta)) {
                return "ERROR|Cuenta no encontrada: ".concat(idCuenta);
            }
            
            var lock = this.adquirirLockCuenta(idCuenta);
            if (!lock) {
                return 'ERROR|No se pudo adquirir lock para la cuenta';
            }
            
            try {
                var saldo = this.cuentas.get(idCuenta).get('saldo');
                return "OK|".concat(saldo.toFixed(2));
            } finally {
                this.liberarLockCuenta(idCuenta);
            }
        } catch (err) {
            this.log("Error consultando saldo: ".concat(err));
            return "ERROR|".concat(err);
        }
    };

    /**
     * Realiza una transferencia entre cuentas
     */
    NodoTrabajadorJs.prototype.transferirFondos = function (parametros) {
        if (parametros.length < 3) {
            return 'ERROR|Faltan parámetros para transferencia';
        }
        
        try {
            var cuentaOrigen = parseInt(parametros[0]);
            var cuentaDestino = parseInt(parametros[1]);
            var monto = parseFloat(parametros[2]);
            
            if (monto <= 0) {
                return 'ERROR|El monto debe ser positivo';
            }
            
            if (!this.cuentas.has(cuentaOrigen)) {
                return "ERROR|Cuenta origen no encontrada: ".concat(cuentaOrigen);
            }
            
            if (!this.cuentas.has(cuentaDestino)) {
                return "ERROR|Cuenta destino no encontrada: ".concat(cuentaDestino);
            }
            
            var locks = this.adquirirLocksOrdenados(cuentaOrigen, cuentaDestino);
            if (!locks) {
                return 'ERROR|No se pudieron adquirir locks para las cuentas';
            }
            
            try {
                var saldoOrigen = this.cuentas.get(cuentaOrigen).get('saldo');
                if (saldoOrigen < monto) {
                    return 'ERROR|Saldo insuficiente';
                }
                
                this.cuentas.get(cuentaOrigen).set('saldo', saldoOrigen - monto);
                var saldoDestino = this.cuentas.get(cuentaDestino).get('saldo');
                this.cuentas.get(cuentaDestino).set('saldo', saldoDestino + monto);
                
                if (!this.adquirirLockTransaccion()) {
                    return 'ERROR|No se pudo adquirir lock para registrar la transacción';
                }
                
                try {
                    var idTransaccion = this.transacciones.length + 1;
                    var now = new Date();
                    var fechaHora = "".concat(now.getFullYear(), "-").concat(String(now.getMonth() + 1).padStart(2, '0'), "-").concat(String(now.getDate()).padStart(2, '0'), " ").concat(String(now.getHours()).padStart(2, '0'), ":").concat(String(now.getMinutes()).padStart(2, '0'), ":").concat(String(now.getSeconds()).padStart(2, '0'));
                    
                    var transaccion = new Map();
                    transaccion.set('id_transacc', idTransaccion);
                    transaccion.set('id_orig', cuentaOrigen);
                    transaccion.set('id_dest', cuentaDestino);
                    transaccion.set('monto', monto);
                    transaccion.set('fecha_hora', fechaHora);
                    transaccion.set('estado', 'Confirmada');
                    this.transacciones.push(transaccion);
                    
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
            this.log("Error en transferencia: ".concat(err));
            return "ERROR|".concat(err);
        }
    };

    // Métodos de lock (sin cambios)
    NodoTrabajadorJs.prototype.adquirirLockCuenta = function (idCuenta) {
        var intentos = 0;
        var maxIntentos = 50;
        while (intentos < maxIntentos) {
            if (!this.cuentaLocks.get(idCuenta)) {
                this.cuentaLocks.set(idCuenta, true);
                return true;
            }
            this.sleep(100);
            intentos++;
        }
        return false;
    };

    NodoTrabajadorJs.prototype.liberarLockCuenta = function (idCuenta) {
        this.cuentaLocks.set(idCuenta, false);
    };

    NodoTrabajadorJs.prototype.adquirirLocksOrdenados = function (cuenta1, cuenta2) {
        var _a = cuenta1 < cuenta2 ? [cuenta1, cuenta2] : [cuenta2, cuenta1], primera = _a[0], segunda = _a[1];
        if (!this.adquirirLockCuenta(primera)) {
            return false;
        }
        if (!this.adquirirLockCuenta(segunda)) {
            this.liberarLockCuenta(primera);
            return false;
        }
        return true;
    };

    NodoTrabajadorJs.prototype.adquirirLockTransaccion = function () {
        var intentos = 0;
        var maxIntentos = 50;
        while (intentos < maxIntentos) {
            if (!this.transaccionLock) {
                this.transaccionLock = true;
                return true;
            }
            this.sleep(100);
            intentos++;
        }
        return false;
    };

    NodoTrabajadorJs.prototype.liberarLockTransaccion = function () {
        this.transaccionLock = false;
    };

    NodoTrabajadorJs.prototype.sleep = function (ms) {
        var start = Date.now();
        while (Date.now() - start < ms) {
            // Esperar
        }
    };

    /**
     * Registra un mensaje de log
     */
    NodoTrabajadorJs.prototype.log = function (mensaje) {
        var now = new Date();
        var timestamp = "".concat(now.getFullYear(), "-").concat(String(now.getMonth() + 1).padStart(2, '0'), "-").concat(String(now.getDate()).padStart(2, '0'), " ").concat(String(now.getHours()).padStart(2, '0'), ":").concat(String(now.getMinutes()).padStart(2, '0'), ":").concat(String(now.getSeconds()).padStart(2, '0'));
        var logLine = "[".concat(timestamp, "] ").concat(mensaje);
        
        console.log(logLine);
        
        try {
            fs.appendFileSync(this.LOG_FILE, logLine + '\n');
        } catch (err) {
            console.error("Error escribiendo en log: ".concat(err));
        }
    };

    /**
     * Cierra el SERVER
     */
    NodoTrabajadorJs.prototype.cerrar = function () {
        if (this.server) {
            this.server.close();
            this.log('SERVER cerrado');
        }
    };

    return NodoTrabajadorJs;
}());

// Punto de entrada
function main() {
    var args = process.argv.slice(2);
    var idNodo;
    var ipServer;
    
    if (args.length >= 1) {
        idNodo = parseInt(args[0]);
    }
    if (args.length >= 2) {
        ipServer = args[1];
    }
    
    var nodo = new NodoTrabajadorJs(idNodo, ipServer);
    
    process.on('SIGINT', function () {
        console.log('\nCerrando el nodo trabajador...');
        nodo.cerrar();
        process.exit(0);
    });
    
    process.on('SIGTERM', function () {
        console.log('\nCerrando el nodo trabajador...');
        nodo.cerrar();
        process.exit(0);
    });
}

// Ejecutar el programa
main();

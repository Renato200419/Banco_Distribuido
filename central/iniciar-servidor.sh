#!/bin/bash
# Script para compilar y ejecutar el servidor central

# Crear estructura de directorios si no existe
mkdir -p ../data/{clientes,cuentas,transacciones}
mkdir -p ../logs

# Compilar los archivos Java
echo "Compilando ServidorCentral.java..."
javac ServidorCentral.java

echo "Compilando Cliente.java..."
javac Cliente.java

# Crear archivos de datos iniciales si no existen
if [ ! -f "../data/clientes/clientes.txt" ]; then
    echo "Creando archivo de clientes inicial..."
    echo "1|Juan Pérez|juan@email.com|987654321" > ../data/clientes/clientes.txt
    echo "2|María López|maria@email.com|998877665" >> ../data/clientes/clientes.txt
    echo "3|Carlos Rodríguez|carlos@email.com|912345678" >> ../data/clientes/clientes.txt
fi

if [ ! -f "../data/cuentas/cuentas.txt" ]; then
    echo "Creando archivo de cuentas inicial..."
    echo "101|1|1500.00|Ahorros" > ../data/cuentas/cuentas.txt
    echo "102|2|3200.50|Corriente" >> ../data/cuentas/cuentas.txt
    echo "103|3|2100.75|Ahorros" >> ../data/cuentas/cuentas.txt
fi

if [ ! -f "../data/transacciones/transacciones.txt" ]; then
    echo "Creando archivo de transacciones inicial..."
    touch ../data/transacciones/transacciones.txt
fi

# Mostrar la IP de la máquina para compartir con los compañeros
echo "====================== INFORMACIÓN DE RED ======================"
echo "La IP de este servidor es: $(hostname -I | awk '{print $1}')"
echo "Esta información es importante para que tus compañeros"
echo "puedan conectar sus nodos trabajadores a este servidor central."
echo "==============================================================="

# Iniciar el servidor
echo "Iniciando Servidor Central..."
java ServidorCentral
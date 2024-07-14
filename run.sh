#!/bin/bash

# Compilar o projeto usando Maven
echo "Compilando o projeto..."
mvn clean package

# Verificar se o JAR foi gerado com sucesso
if [ ! -f "target/spark-setup-1.0-SNAPSHOT.jar" ]; then
    echo "Falha na compilação do projeto. JAR não encontrado."
    exit 1
fi

# Executar o JAR gerado
echo "Executando a aplicação..."
java -jar target/spark-setup-1.0-SNAPSHOT.jar
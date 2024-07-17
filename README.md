# Spark_App

Este projeto é uma aplicação Spark desenvolvida em Scala que processa dados de aplicações e a análise de utilizadores da Play Store.

## Requisitos

Este projeto foi executado com as seguintes ferramentas:

- Java 8
- Scala 2.12.18
- Apache Spark 3.5.1
- Maven 3.9.8

## Configuração

### Passo 1: Clonar o Repositório

```sh
git clone https://github.com/joaolouro02/Spark_App.git
cd .\Spark_App\
```

### Passo 2: Compilar o código-fonte

```sh
mvn clean package
```

### Passo 3: Executar a aplicação:

```sh
java -jar target/spark-setup-1.0-SNAPSHOT.jar
```

## Execução no IDE

### Passo 1: Editar a configurção
![img1](https://github.com/user-attachments/assets/b12da6ad-c4b1-40f7-9134-488bdbd9fc2b)

### Passo 2: Adicionar VM Options
<img src="https://github.com/user-attachments/assets/3411bf97-c590-4951-bc41-b36e6e45683d" alt="img2" width="300" height="200"/>

```sh
--add-opens java.base/sun.nio.ch=ALL-UNNAMED --add-opens java.base/sun.util.calendar=ALL-UNNAMED
```

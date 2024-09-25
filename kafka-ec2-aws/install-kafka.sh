#!/bin/bash
echo "Atualizando o sistema..."
sudo yum update -y
echo "Instalando java...."
sudo yum install java -y
echo "Instalando o wget..."
sudo yum install wget -y
sudo yum install maven -y
cd /home/ec2-user
echo "Instalando o kafka..."
wget https://downloads.apache.org/kafka/3.8.0/kafka_2.12-3.8.0.tgz
tar -xzf kafka_2.12-3.8.0.tgz
echo "Configurando permissões..."
sudo chmod -R 777 kafka_2.12-3.8.0
sudo chown -R ec2-user:ec2-user kafka_2.12-3.8.0
echo "Instalando o git..."
sudo yum install git -y
echo "Baixando repositório..."
git clone https://github.com/evertonmj/howtocode_bigdata.git

wget https://github.com/provectus/kafka-ui/releases/download/v0.7.2/kafka-ui-api-v0.7.2.jar
#https://docs.kafka-ui.provectus.io/development/building/without-docker

mkdir -p /home/ec2-user/conf

echo """
logging:
  level:
    root: INFO
    com.provectus: DEBUG
    #org.springframework.http.codec.json.Jackson2JsonEncoder: DEBUG
    #org.springframework.http.codec.json.Jackson2JsonDecoder: DEBUG
    reactor.netty.http.server.AccessLog: INFO
    org.springframework.security: DEBUG

server:
  port: 8080 #- Port in which kafka-ui will run.

kafka:
  clusters:
    -
      name: local
      bootstrapServers: localhost:9092
""" > /home/ec2-user/conf/application.yaml

sudo chown -R ec2-user:ec2-user /home/ec2-user
sudo chmod -R 755 /home/ec2-user

# java -Dspring.config.additional-location=conf/application.yaml --add-opens java.rmi/javax.rmi.ssl=ALL-UNNAMED -jar kafka-ui-api-v0.7.2.jar


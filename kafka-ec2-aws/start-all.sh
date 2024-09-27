!#bash
export UHOME=/home/ec2-user
export KAFKA_HOME=/home/ec2-user/kafka_2.12-3.8.0
echo "Iniciando o zookeeper..."
$KAFKA_HOME/bin/zookeeper-server-start.sh -daemon $KAFKA_HOME/config/zookeeper.properties
echo "Iniciando o kafka..."
$KAFKA_HOME/bin/kafka-server-start.sh -daemon $KAFKA_HOME/config/server.properties
echo "Checando status..."

nohup java -Dspring.config.additional-location=conf/application.yaml --add-opens java.rmi/javax.rmi.ssl=ALL-UNNAMED -jar $UHOME/kafka-ui-api-v0.7.2.jar &

{
	"id": "bitcoin",
	"symbol": "BTC",
	"name": "Bitcoin",
	"current_price": 65068.79,
	"market_cap": 1285721230145.53,
	"price_change_percentage_24h": 2.64
}
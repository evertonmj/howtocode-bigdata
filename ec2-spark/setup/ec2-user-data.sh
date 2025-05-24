#!/bin/bash
sudo apt update
sudo apt install unzip openjdk-11-jdk python3-pip git -y
sudo snap install aws-cli --classic

cd /home/ubuntu
mkdir spark && cd spark
wget https://dlcdn.apache.org/spark/spark-3.5.5/spark-3.5.5-bin-hadoop3.tgz
tar -xzvf spark-3.5.5-bin-hadoop3.tgz 
sudo mv spark-3.5.5-bin-hadoop3 /opt/spark
echo "export SPARK_HOME=/opt/spark" >> ~/.bashrc
echo "export PATH=\$SPARK_HOME/bin:\$PATH" >> ~/.bashrc
echo "export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64" >> ~/.bashrc
source /home/ubuntu/.bashrc

cd /opt/spark/jars/
wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sd-k-bundle/1.12.540/aws-java-sdk-bundle-1.12.540.jar
wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-common/3.3.6/hadoop-common-3.3.6.jar/
wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.6/hadoop-aws-3.3.6.jar
wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-common/3.3.6/hadoop-common-3.3.6.jar
wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.540/aws-java-sdk-bundle-1.12.540.jar
wget https://repo1.maven.org/maven2/com/fasterxml/woodstox/woodstox-core/6.2.6/woodstox-core-6.2.6.jar
wget https://repo1.maven.org/maven2/org/codehaus/woodstox/stax2-api/4.2/stax2-api-4.2.jar
wget https://repo1.maven.org/maven2/org/apache/commons/commons-configuration2/2.9.0/commons-configuration2-2.9.0.jar
wget https://repo1.maven.org/maven2/org/apache/commons/commons-text/1.10.0/commons-text-1.10.0.jar

mkdir /home/ubuntu/jobs
cd /home/ubuntu/jobs
curl -L -o steam-reviews.zip  https://www.kaggle.com/api/v1/datasets/download/andrewmvd/steam-reviews
unzip steam-reviews.zip

aws s3 cp dataset.csv s3://evert-bg-01/data/dataset.csv

cd /home/ubuntu/jobs
wget https://raw.githubusercontent.com/evertonmj/howtocode-bigdata/refs/heads/main/ec2-spark/jobs/job1.py
wget https://raw.githubusercontent.com/evertonmj/howtocode-bigdata/refs/heads/main/ec2-spark/jobs/previsao-score-nlp.py

sudo  chmod -R 777 /home/ubuntu/

cd /home/ubuntu
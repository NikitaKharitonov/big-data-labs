# Lab #1 - Introduction to Apache Spark - for Big Data course
- Scala 2.12.9
- sbt 1.5.5
- JDK 1.8.0_302 (local)
- JDK 11.0.8 (server)
- Apache Spark 2.4.5

Задание https://gitlab.com/ssau.tk.courses/big_data/-/blob/master/L1%20-%20Introduction%20to%20Apache%20Spark/L1_Apache_Spark.md.

Было произведено развёртывание MapR с помощью Docker в WSL2 Ubuntu 18.04 с помощью следующих команд.
```shell 
sudo rm -rf /tmp/maprdemo

sudo mkdir -p /tmp/maprdemo/hive /tmp/maprdemo/zkdata /tmp/maprdemo/pid /tmp/maprdemo/logs /tmp/maprdemo/nfs
sudo chmod -R 777 /tmp/maprdemo/hive /tmp/maprdemo/zkdata /tmp/maprdemo/pid /tmp/maprdemo/logs /tmp/maprdemo/nfs

export clusterName="maprdemo.mapr.io"
export MAPR_EXTERNAL='0.0.0.0'
export PORTS='-p 9998:9998 -p 8042:8042 -p 8888:8888 -p 8088:8088 -p 9997:9997 -p 10001:10001 -p 8190:8190 -p 8243:8243 -p 2222:22 -p 4040:4040 -p 7221:7221 -p 8090:8090 -p 5660:5660 -p 8443:8443 -p 19888:19888 -p 50060:50060 -p 18080:18080 -p 8032:8032 -p 14000:14000 -p 19890:19890 -p 10000:10000 -p 11443:11443 -p 12000:12000 -p 8081:8081 -p 8002:8002 -p 8080:8080 -p 31010:31010 -p 8044:8044 -p 8047:8047 -p 11000:11000 -p 2049:2049 -p 8188:8188 -p 7077:7077 -p 7222:7222 -p 5181:5181 -p 5661:5661 -p 5692:5692 -p 5724:5724 -p 5756:5756 -p 10020:10020 -p 50000-50050:50000-50050 -p 9001:9001 -p 5693:5693 -p 9002:9002 -p 31011:31011'

docker run --name maprdemo -d --privileged -v /tmp/maprdemo/zkdata:/opt/mapr/zkdata -v /tmp/maprdemo/pid:/opt/mapr/pid  -v /tmp/maprdemo/logs:/opt/mapr/logs  -v /tmp/maprdemo/nfs:/mapr $PORTS -e MAPR_EXTERNAL -e clusterName -e isSecure --hostname ${clusterName} maprtech/dev-sandbox-container:latest > /dev/null 2>&1

sleep 400

docker exec -d maprdemo usermod -s /bin/bash root
docker exec -d maprdemo usermod -s /bin/bash mapr
docker exec -d maprdemo apt install -y mapr-resourcemanager mapr-nodemanager mapr-historyserver
docker exec -d maprdemo /opt/mapr/server/configure.sh -R

```


Было добавлено расположение Spark-утилит в переменную среды PATH для их вызова без указания полного пути.
```shell
export PATH=$PATH:/opt/mapr/spark/spark-2.4.5/bin
```


Программа была написана и отлажена на языке Scala в среде разработки Intellij IDEA с 
использованием системы автоматической сборки sbt. Был создан jar-файл проекта с помощью команды
package.
![](images/build.jpg)


Затем собранный jar-файл и файлы с данными были скопированы в запущенный ранее Docker-контейнер по 
протоколу SSH.
![](images/copy.jpg)


Внутри контейнера полученные файлы были размещены в системе HDFS. Проект был запущен на Spark-кластере.
![](images/deploy.jpg)


Были получены следующие логи программы.
![](images/logs1.jpg)
![](images/logs2.jpg)


Таким образом, выдача программы на сервере идентична выдаче в IDE при запуске 
на локальном компьютере.
![](images/idea.jpg)


zookeeper-server-start /usr/local/etc/kafka/zookeeper.properties > /dev/null 2>&1 &
sleep 5s
kafka-server-start /usr/local/etc/kafka/server.properties > /dev/null 2>&1 &
sleep 5s
cassandra -f > /dev/null 2>&1 &
sleep 5s

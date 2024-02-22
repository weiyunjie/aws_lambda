from kafka import KafkaConsumer
import time

kafka_hosts = ["192.168.1.1:9092","192.168.1.2:9092","192.168.1.3:9092"] #业务新节点

kafka_topic = "lambda"      #topic name
consumer = KafkaConsumer(kafka_topic, group_id="test", bootstrap_servers=kafka_hosts, ssl_check_hostname=False)  # 要设置api_version否则可能会报错，如果没有使用ssl认证设置为False

# 方式一：
#
# print("t1", time.time())  # 记录拉取时间
# while True:
#     print("t2", time.time())
#     msg = consumer.poll(timeout_ms=100, max_records=5)  # 从kafka获取消息，每0.1秒拉取一次，单次拉取5条
#     print(len(msg))
#
#     for i in msg.values():
#         for k in i:
#             print(k.offset, k.value)
#     time.sleep(1)

# 方式二：

for message in consumer:
    if message is not None:
        print("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
                                             message.offset, message.key,
                                             message.value.decode()))
    else:
        print("30s内未接受到数据")
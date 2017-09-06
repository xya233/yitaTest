package java;

import kafka.consumer.ConsumerConfig;

import java.util.Properties;

class KafkaConsumerConfig {
	public ConsumerConfig consumerConfig;

	{
		Properties kafkaProperties = new Properties();
		kafkaProperties.put("group.id", "group-987");
		kafkaProperties.put("socket.receive.buffer.bytes", "2097152");
		kafkaProperties.put("fetch.message.max.bytes", "1048576");
		kafkaProperties.put("zookeeper.connect", "lenovo-0-1:5214,lenovo-0-5:5214,lenovo-0-6:5214,lenovo-0-8:5214,lenovo-0-0:5214");
		kafkaProperties.put("consumer.timeout.ms", "-1");
		kafkaProperties.put("num.consumer.fetchers", "1");
		kafkaProperties.put("auto.offset.reset", "smallest");	//可选参数值：	largest	smallest
		consumerConfig = new ConsumerConfig(kafkaProperties);
	}

	public KafkaConsumerConfig() {
	}

}

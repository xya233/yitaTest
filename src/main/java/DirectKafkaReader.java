package java;

import cn.com.jetflow.yita.*;
import com.google.common.base.Preconditions;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import org.jctools.queues.SpscArrayQueue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

public class DirectKafkaReader extends Flowlet {
	private final Map<String, Integer> topicPartitionCountMap;

	public DirectKafkaReader(Map<String, Integer> topicPartitionCountMap) {
		super();
		this.topicPartitionCountMap = topicPartitionCountMap;
	}

	@Override
		public void prepare(List<Partition> partitions) {
			super.prepare(partitions);
			java.KafkaConsumerConfig config = new java.KafkaConsumerConfig();
			ConsumerConnector consumerConnector = Consumer.createJavaConsumerConnector(config.consumerConfig);

			Map<String, Integer> topicLocalPartitionCount = new HashMap<>();

			int slaveNum = this.getCurrentSlave().getSlaveCount();
			int currentSlaveId = this.getCurrentSlave().getID();

			Iterator<String> topicSet = this.topicPartitionCountMap.keySet().iterator();
			String[] topics = new String[topicPartitionCountMap.keySet().size()];
			for (int i = 0; i < topics.length; i++) {
				topics[i] = topicSet.next();
			}
			int currentPos = 0;
			for (int i = 0; i < topics.length; i++) {
				String topic = topics[i];
				topicLocalPartitionCount.put(topic, 0);
				for (int j = 0; j < topicPartitionCountMap.get(topic); j++) {
					if (currentPos % slaveNum == currentSlaveId) {
						topicLocalPartitionCount.put(topic, topicLocalPartitionCount.get(topic) + 1);
					}
					currentPos++;
				}
			}

			Map<String, List<KafkaStream<byte[], byte[]>>> topicStreamMap = consumerConnector
				.createMessageStreams(topicLocalPartitionCount);
			List<KafkaStream<byte[], byte[]>> streams = new ArrayList<>();

			for (Map.Entry<String, List<KafkaStream<byte[], byte[]>>> streamList : topicStreamMap.entrySet()) {
				streams.addAll(streamList.getValue());
			}

			int currentLocalPartitionIndex = 0;
			for (KafkaStream<byte[], byte[]> stream : streams) {
				if (currentLocalPartitionIndex >= partitions.size()) {
					currentLocalPartitionIndex = currentLocalPartitionIndex % partitions.size();
				}
				((PartitionWithKafkaStream) partitions.get(currentLocalPartitionIndex)).addStream(stream);
				currentLocalPartitionIndex++;
			}

			for (Partition partition : partitions) {
				((PartitionWithKafkaStream) partition).build();
			}

		}

	@Override
		public PartitionWithKafkaStream newPartition(int partitionId) {
			return new PartitionWithKafkaStream(this, partitionId);
		}

	public final ProducerPushPort<byte[], byte[]> out = add(new ProducerPushPort<byte[], byte[]>() {
			@Override
			public KeyValueProducer<byte[], byte[]> newProducer(Partition partition) {
			return new KeyValueProducer<byte[], byte[]>() {
			PartitionWithKafkaStream partitionWithKafkaStream;

			@Override
			public boolean prepare(ITask task) {
			partitionWithKafkaStream = (PartitionWithKafkaStream) task.getPartition();
			if (partitionWithKafkaStream.getConsumerIteratorNum() == 0) {
			return false;
			}
			return true;
			}

			@Override
			public State produce(DedicatedFlow<byte[], byte[]> dedicatedFlow) throws IOException {
			MessageAndMetadata<byte[], byte[]> result = partitionWithKafkaStream.produce();
			if (result != null) {

			String line = new String(result.message(),StandardCharsets.UTF_8);
			//System.out.println(line);
			dedicatedFlow.push(result.key(), result.message());
			return State.CONTINUE;
			} else {
			//dedicatedFlow.push("aaa".getBytes(), "bbb".getBytes());
				return State.RESCHEDULE;
			}
			}
			};
			}
	});

	private class PartitionWithKafkaStream extends Partition {
		private ConsumerIterator<byte[], byte[]>[] consumerIterators;
		private final List<ConsumerIterator<byte[], byte[]>> rawConsumerIterators = new ArrayList<>();
		private int consumerIteratorNum = 0;
		private SpscArrayQueue<MessageAndMetadata<byte[], byte[]>> waitingMessages = new SpscArrayQueue<>(20480);
		private final AtomicReference<Thread> waitingFetcherThread = new AtomicReference<>();

		private PartitionWithKafkaStream(Flowlet flowlet, int id) {
			super(flowlet, id);
		}

		private MessageAndMetadata<byte[], byte[]> produce() {
			MessageAndMetadata<byte[], byte[]> messageAndMetadata = waitingMessages.poll();
			if (waitingFetcherThread.get() != null) {
				LockSupport.unpark(waitingFetcherThread.getAndSet(null));
			}
			return messageAndMetadata;
		}

		public void addStream(KafkaStream<byte[], byte[]> stream) {
			this.rawConsumerIterators.add(stream.iterator());
		}

		@SuppressWarnings("unchecked")
			public void build() {
				consumerIterators = new ConsumerIterator[rawConsumerIterators.size()];
				rawConsumerIterators.toArray(consumerIterators);
				consumerIteratorNum = consumerIterators.length;
				if (consumerIteratorNum > 0) {
					ExecutorService fetcher = Executors.newSingleThreadExecutor(new ThreadFactory() {
							@Override
							public Thread newThread(Runnable r) {
							Thread t = new Thread(r);
							t.setName("fetcher" + PartitionWithKafkaStream.this.getId());
							t.setDaemon(true);
							return t;
							}
							});
					fetcher.submit(new Runnable() {
							@Override
							public void run() {
							int currentConsumerIteratorIndex = 0;
							while (true) {
							try {
							Preconditions.checkArgument(consumerIterators[currentConsumerIteratorIndex].hasNext());
							final MessageAndMetadata<byte[], byte[]> messageAndMetadata = consumerIterators[currentConsumerIteratorIndex]
							.next();
							while (!waitingMessages.offer(messageAndMetadata)) {
							waitingFetcherThread.compareAndSet(null, Thread.currentThread());
							LockSupport.park();
							}
							currentConsumerIteratorIndex++;
							if (currentConsumerIteratorIndex == consumerIteratorNum) {
							currentConsumerIteratorIndex = 0;
							}
							} catch (Throwable throwable) {
							LOG.error("thrown in Kafka fetcher", throwable);
							}
							}
							}
					});
				}
			}

		public int getConsumerIteratorNum() {
			return consumerIteratorNum;
		}
	}}


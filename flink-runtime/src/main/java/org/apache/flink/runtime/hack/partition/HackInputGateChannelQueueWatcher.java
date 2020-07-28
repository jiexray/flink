package org.apache.flink.runtime.hack.partition;

import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.hack.HackStringUtil;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This is class is for monitoring the inputChannelWithData queue in
 * {@link org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate}.
 * Further, we tick the timestamps of queueChannel() and getChannel() in
 * {@link org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate}
 * to compute the queue delay of an inputChannel.
 */
public class HackInputGateChannelQueueWatcher {
	/**
	 * A class for identifying an inputChannel in a global view.
	 * There may be multiple StreamTask in a given TaskManager, which results in
	 * multiple InputGate. An inputChannel will have a same ChannelInfo, for example
	 * InputChannelInfo {gateIdx=0, inputChannelIdx=0}.
	 */
	private static class GlobalInputChannelInfo {
		private String owningTaskName;
		private InputChannelInfo channelInfo;

		public GlobalInputChannelInfo(String owningTaskName, InputChannelInfo inputChannelInfo) {
			this.owningTaskName = owningTaskName;
			this.channelInfo = inputChannelInfo;
		}

		@Override
		public String toString() {
			return "GlobalInputChannelInfo{" +
				"owningTaskName='" + owningTaskName + '\'' +
				", channelInfo=" + channelInfo +
				'}';
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			GlobalInputChannelInfo that = (GlobalInputChannelInfo) o;
			return Objects.equals(owningTaskName, that.owningTaskName) &&
				Objects.equals(channelInfo, that.channelInfo);
		}

		@Override
		public int hashCode() {
			return Objects.hash(owningTaskName, channelInfo);
		}
	}

	private static Map<GlobalInputChannelInfo, Long> inputChannelToQueueTimeStamp = new ConcurrentHashMap<>();

	private static Object lock = new Object();

	public static void dumpLengthOfInputChannelWithData(SingleInputGate inputGate, InputChannel inputChannel, boolean queueOrGet) {
		if (queueOrGet) {
			printQueueChannel(inputGate, inputChannel);
		} else {
			printGetChannel(inputGate, inputChannel);
		}
	}

	private static void printQueueChannel(SingleInputGate inputGate, InputChannel inputChannel) {
		String channelInfo = HackStringUtil.convertInputChannelToString(inputChannel);
		System.out.println("SingleInputGate [" + HackStringUtil.convertInputGateToString(inputGate) +
			"] queue of inputChannelWithData is [" + inputGate.getNumberOfInputChannelWithData() +
			"] in length " + "after queueChannel() for Channel [" + channelInfo + "]");
	}

	private static void printQueueMoreAvailable(SingleInputGate inputGate, InputChannel inputChannel) {
		String channelInfo = HackStringUtil.convertInputChannelToString(inputChannel);
		System.out.println("SingleInputGate [" + HackStringUtil.convertInputGateToString(inputGate) +
			"] queue of inputChannelWithData is [" + inputGate.getNumberOfInputChannelWithData() +
			"] in length " + "after queue channel for more available for Channel [" + channelInfo + "]");
	}

	private static void printGetChannel(SingleInputGate inputGate, InputChannel inputChannel) {
		String channelInfo = HackStringUtil.convertInputChannelToString(inputChannel);
		System.out.println("SingleInputGate [" + HackStringUtil.convertInputGateToString(inputGate) +
			"] queue of inputChannelWithData is [" + inputGate.getNumberOfInputChannelWithData() +
			"] in length " + "after getChannel() for Channel [" + channelInfo + "]");
	}

	public static void tickInputChannelMoreAvailable(SingleInputGate inputGate, InputChannel inputChannel) {
		tickInputChannelQueueTimestamp(inputChannel);
		printQueueMoreAvailable(inputGate, inputChannel);
	}

	public static void tickInputChannelQueueTimestamp(InputChannel inputChannel) {
		String owningTaskName = inputChannel.getInputGate().getOwningTaskName();
		synchronized (lock) {
			GlobalInputChannelInfo globalInputChannelInfo = new GlobalInputChannelInfo(owningTaskName, inputChannel.getChannelInfo());
			if (inputChannelToQueueTimeStamp.containsKey(globalInputChannelInfo)) {
				System.out.println("[ERROR!!!] Never add a same InputChannel to the inputChannelWithData queue");
				return;
			} else {
				inputChannelToQueueTimeStamp.put(globalInputChannelInfo, System.currentTimeMillis());
			}
		}
	}

	public static void tickInputChannelGetTimestamp(InputChannel inputChannel, Optional<InputChannel.BufferAndAvailability> result) {
		int bufferSize = 0;
		if (result.isPresent()) {
			bufferSize = result.get().buffer().getSize();
		}

		String owningTaskName = inputChannel.getInputGate().getOwningTaskName();
		synchronized (lock) {
			GlobalInputChannelInfo globalInputChannelInfo = new GlobalInputChannelInfo(owningTaskName, inputChannel.getChannelInfo());
			if (inputChannelToQueueTimeStamp.containsKey(globalInputChannelInfo)) {
				long queueTimestamp = inputChannelToQueueTimeStamp.get(globalInputChannelInfo);
				inputChannelToQueueTimeStamp.remove(globalInputChannelInfo);

				String channelInfo = HackStringUtil.convertInputChannelToString(inputChannel);
				System.out.println("InputChannel [" + channelInfo + "] has wait from queueChannel() to getChannel() for [" +
					(System.currentTimeMillis() - queueTimestamp) +
					"] ms, and transfer buffer [" + bufferSize + "] Bytes");
			} else {
				System.out.println("[ERROR!!!] have not queued InputChannel [" + inputChannel.getChannelInfo() + "]");
			}
		}
	}
}

package org.apache.flink.runtime.hack.partition;

import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.hack.HackStringUtil;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.LocalInputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.RemoteInputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate;

import java.util.HashMap;
import java.util.Map;

/**
 * This is class is for monitoring the inputChannelWithData queue in
 * {@link org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate}.
 * Further, we tick the timestamps of queueChannel() and getChannel() in
 * {@link org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate}
 * to compute the queue delay of an inputChannel.
 */
public class HackInputGateChannelQueueWatcher {
	private static Map<InputChannelInfo, Long> inputChannelToQueueTimeStamp = new HashMap<>();

	public static void dumpLengthOfInputChannelWithData(SingleInputGate inputGate, boolean queueOrGet) {
		System.out.println("SingleInputGate [" + HackStringUtil.convertInputGateToString(inputGate) +
			"] queue of inputChannelWithData is [" + inputGate.getNumberOfInputChannelWithData() +
			"] in length during " + (queueOrGet ? "[queueChannel()]" : "[getChannel()]"));
	}

	public static void tickInputChannelQueueTimestamp(InputChannel inputChannel) {
		if (inputChannelToQueueTimeStamp.containsKey(inputChannel.getChannelInfo())) {
			return;
		} else {
			inputChannelToQueueTimeStamp.put(inputChannel.getChannelInfo(), System.currentTimeMillis());
		}
	}

	public static void tickInputChannelGetTimestamp(InputChannel inputChannel) {
		long queueTimestamp = inputChannelToQueueTimeStamp.get(inputChannel.getChannelInfo());
		if (inputChannelToQueueTimeStamp.containsKey(inputChannel.getChannelInfo())) {
			if (inputChannel instanceof LocalInputChannel) {
				LocalInputChannel localInputChannel = (LocalInputChannel) inputChannel;
				System.out.println("LocalInputChannel [" + HackStringUtil.convertLocalInputChannelToString(localInputChannel) +
					"] has wait from queueChannel() to getChannel for [" + (System.currentTimeMillis() - queueTimestamp) +
					"] ms");
			} else if (inputChannel instanceof RemoteInputChannel) {
				RemoteInputChannel remoteInputChannel = (RemoteInputChannel) inputChannel;
				System.out.println("RemoteInputChannel [" + HackStringUtil.convertRemoteInputChannelToString(remoteInputChannel) +
					"] has wait from queueChannel() to getChannel for [" + (System.currentTimeMillis() - queueTimestamp) +
					"] ms");
			} else {
				System.out.println("[ERROR!!!] cannot extract the InputChannel [" + inputChannel.getChannelInfo() + "]");
			}
		} else {
			System.out.println("[ERROR!!!] have not queued InputChannel [" + inputChannel.getChannelInfo() + "]");
		}
	}
}

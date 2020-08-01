package org.apache.flink.runtime.hack.partition;

import org.apache.flink.runtime.hack.HackConfig;
import org.apache.flink.runtime.io.network.partition.consumer.RemoteInputChannel;
import org.apache.flink.shaded.netty4.io.netty.channel.Channel;

/**
 * This is a time recorder for:
 * 1. [deprecate] requesting remote partition;
 * 2. remote partition onBuffer (receive);
 * 3. remote partition send;
 */
public class HackRemotePartitionTimeRecorder {
	public static void tickOnBufferReceived(RemoteInputChannel inputChannel, int sequenceNumber, int bufferSize) {
		if (HackConfig.hackAll) {
			doTickOnBufferReceived(inputChannel, sequenceNumber, bufferSize);
		}
	}

	public static void doTickOnBufferReceived(RemoteInputChannel inputChannel, int sequenceNumber, int bufferSize) {
		long currentTs = System.currentTimeMillis();
		String inputChannelId = inputChannel.getInputChannelId().toString();

		System.out.println("[RemoteInputChannelId,sequenceNumber,receiveOrSend,timestamp,bufferSize]: [" + inputChannelId + "," + sequenceNumber +
			",1," + currentTs + "," + bufferSize + "]");
	}

	public static void tickOnBufferSend(Channel channel, int sequenceNumber, int bufferSize, String inputChannelId) {
		if (HackConfig.hackAll) {
			doTickOnBufferSend(channel, sequenceNumber, bufferSize, inputChannelId);
		}
	}

	public static void doTickOnBufferSend(Channel channel, int sequenceNumber, int bufferSize, String inputChannelId) {
		long currentTs = System.currentTimeMillis();

		System.out.println("[RemoteInputChannelId, sequenceNumber,receiveOrSend,timestamp,bufferSize]: [" + inputChannelId + "," + sequenceNumber +
			",0," + currentTs + "," + bufferSize + "]");
	}
}

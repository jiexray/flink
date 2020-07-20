package org.apache.flink.runtime.hack.partition;

import org.apache.flink.runtime.hack.HackStringUtil;
import org.apache.flink.runtime.io.network.partition.consumer.RemoteInputChannel;

import java.util.HashMap;
import java.util.Map;

/**
 * This is a time recorder for:
 * 1. requesting remote partition;
 * 2. remote partition onBuffer;
 */
public class HackRemotePartitionTimeRecorder {
	private static Map<Integer, Long> remotePartitionRequestTimestamps = new HashMap<>();
	private static Map<Integer, Long> bufferReceivedTimestamps = new HashMap<>();
	private static Map<Integer, String> remoteInputChannelInfo = new HashMap<>();

	private static int globalRequestId = 0;

	private static Object lock = new Object();

	public static int getNextRequestId() {
		synchronized (lock) {
			return globalRequestId++;
		}
	}

	public static void registerInputChannelInfo(int requestId, RemoteInputChannel inputChannel) {
		remoteInputChannelInfo.put(requestId, HackStringUtil.convertRemoteInputChannelToString(inputChannel));
	}

	public static void tickRemotePartitionRequest(int requestId) {
		long currentTs = System.currentTimeMillis();
		remotePartitionRequestTimestamps.put(requestId, currentTs);
	}

	public static void tickBufferReceived(int requestId) {
		long currentTs = System.currentTimeMillis();
		bufferReceivedTimestamps.put(requestId, currentTs);

		analysisRequestDelay(requestId);
	}

	private static void analysisRequestDelay(int requestId) {
		long requestTs = remotePartitionRequestTimestamps.get(requestId);
		long receivedTs = bufferReceivedTimestamps.get(requestId);
		String inputChannelInfo = remoteInputChannelInfo.get(requestId);

		long interval = receivedTs - requestTs;

		System.out.println("Remote partition request [" + inputChannelInfo +
			"], delay [" + interval + "] ms");
	}
}

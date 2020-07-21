package org.apache.flink.runtime.hack.partition;

import org.apache.flink.runtime.hack.HackStringUtil;
import org.apache.flink.runtime.io.network.partition.consumer.LocalInputChannel;

/**
 * Time recorder for local partition request:
 * 1. buffer notify;
 * 2. buffer polled;
 */
public class HackLocalPartitionTimeRecorder {
	public static void tickNotifyDataAvailable(LocalInputChannel inputChannel) {
		long currentTs = System.currentTimeMillis();
		System.out.println("LocalInputChannel [" + HackStringUtil.convertLocalInputChannelToString(inputChannel) +
			"] is notified from subpartition view at timestamp [" + currentTs + "]");
	}

	public static void tickDataPolledByLocalInputChannel(LocalInputChannel inputChannel, int bufferSize) {
		long currentTs = System.currentTimeMillis();
		System.out.println("LocalInputChannel [" + HackStringUtil.convertLocalInputChannelToString(inputChannel) +
			"] has polled data [" + bufferSize + "] Bytes from subpartition view at timestamp [" + currentTs + "]");
	}
}

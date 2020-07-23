package org.apache.flink.runtime.hack.partition;

import org.apache.flink.runtime.hack.HackStringUtil;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.ResultSubpartition;
import org.apache.flink.runtime.io.network.partition.consumer.LocalInputChannel;

import java.util.ArrayDeque;
import java.util.Deque;

/**
 * Time recorder for local partition request:
 * 1. buffer notify;
 * 2. buffer polled;
 */
public class HackLocalPartitionTimeRecorder {
	// each (notify, poll) is in a FIFO fashion
	static Deque<Long> notifyTimestamps = new ArrayDeque<>();
	static long buildStartTimestamp;

	public static void tickNotifyDataAvailable(LocalInputChannel inputChannel) {
		long currentTs = System.currentTimeMillis();

		notifyTimestamps.add(currentTs);
//		System.out.println("LocalInputChannel [" + HackStringUtil.convertLocalInputChannelToString(inputChannel) +
//			"] is notified from subpartition view at timestamp [" + currentTs + "]");
		System.out.println("LocalInputChannel [" + HackStringUtil.convertLocalInputChannelToString(inputChannel) +
			"] is notified in Thread [" + Thread.currentThread() + "]");
	}

	public static void tickDataPolledByLocalInputChannel(LocalInputChannel inputChannel, ResultSubpartition.BufferAndBacklog nextBuffer) {
		long currentTs = System.currentTimeMillis();
		int bufferSize = nextBuffer.buffer().getSize();

		if (notifyTimestamps.isEmpty()) {
			Buffer buffer = nextBuffer.buffer();

//			System.out.println("[ERROR!!!] cannot find notify of buffer poll for Buffer type: " + buffer.getDataType());
			return;
		}

		long notifyTs = notifyTimestamps.poll();
//		System.out.println("LocalInputChannel [" + HackStringUtil.convertLocalInputChannelToString(inputChannel) +
//			"] has polled buffer type [" + nextBuffer.buffer().getDataType() + "] from subpartition view at (notify, pool, bufferSize) timestamp: (" +
//			notifyTs + "," + currentTs + "," + bufferSize + ")");
		System.out.println("LocalInputChannel [" + HackStringUtil.convertLocalInputChannelToString(inputChannel) +
			"] has polled buffer in Thread [" + Thread.currentThread() + "]");
	}

	public static void tickBufferConsumerBuildStart() {
		buildStartTimestamp = System.currentTimeMillis();
	}

	public static void tickBufferConsumerBuildEnd(Buffer buffer) {
		System.out.println("Subpartition build buffer type [" + buffer.getDataType() + "] from subpartition view at (notify, pool, bufferSize) timestamp: (" +
			buildStartTimestamp + "," + System.currentTimeMillis() + "," + buffer.getSize() + ")");
	}
}

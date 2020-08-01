package org.apache.flink.runtime.hack.partition;

import org.apache.flink.runtime.hack.HackConfig;
import org.apache.flink.runtime.io.network.buffer.Buffer;

import java.util.ArrayDeque;
import java.util.Deque;

/**
 * Time recorder for local partition request:
 * 1. buffer notify;
 * 2. buffer polled;
 */
public class HackLocalPartitionTimeRecorder {
	// each (notify, poll) is in a FIFO fashion
	static long buildStartTimestamp;

	public static void tickBufferConsumerBuildStart() {
		if (HackConfig.hackAll) {
			buildStartTimestamp = System.currentTimeMillis();
		}
	}

	public static void tickBufferConsumerBuildEnd(Buffer buffer) {
		if (HackConfig.hackAll) {
			System.out.println("Subpartition build buffer type [" + buffer.getDataType() + "] from subpartition view at (notify, pool, bufferSize) timestamp: (" +
				buildStartTimestamp + "," + System.currentTimeMillis() + "," + buffer.getSize() + ")");
		}
	}
}

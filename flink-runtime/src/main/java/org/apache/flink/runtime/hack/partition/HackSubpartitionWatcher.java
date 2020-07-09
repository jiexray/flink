package org.apache.flink.runtime.hack.partition;

import org.apache.flink.runtime.checkpoint.channel.ResultSubpartitionInfo;
import org.apache.flink.runtime.io.network.partition.BufferAvailabilityListener;
import org.apache.flink.runtime.io.network.partition.PipelinedSubpartition;
import org.apache.flink.runtime.io.network.partition.PipelinedSubpartitionView;
import org.apache.flink.runtime.io.network.partition.consumer.LocalInputChannel;

/**
 * A watcher class for {@link org.apache.flink.runtime.io.network.partition.PipelinedSubpartition}
 * and {@link org.apache.flink.runtime.io.network.partition.BoundedBlockingSubpartition}.
 */
public class HackSubpartitionWatcher {
	public static void printPipelinedSubpartitionFlush(PipelinedSubpartition subpartition, PipelinedSubpartitionView dataView) {
		ResultSubpartitionInfo resultSubpartitionInfo = subpartition.getSubpartitionInfo();
		BufferAvailabilityListener availabilityListener = dataView.getAvailabilityListener();

		System.out.println("Subpartition info [" + resultSubpartitionInfo +
		"], " + "flush and notify data available to data viewer [" + dataView +
		"], and trigger BufferAvailableListener " + convertBufferAvailabilityListenerToString(availabilityListener) + "]");
	}

	public static String convertBufferAvailabilityListenerToString(BufferAvailabilityListener availabilityListener) {
		if (availabilityListener == null) {
			return "BufferAvailabilityListener is NULL";
		} else if (availabilityListener instanceof LocalInputChannel) {
			LocalInputChannel localInputChannel = (LocalInputChannel) availabilityListener;
			return localInputChannel.toString();
		} else {
			return "Other type BufferAvailabilityListener";
		}
	}
}

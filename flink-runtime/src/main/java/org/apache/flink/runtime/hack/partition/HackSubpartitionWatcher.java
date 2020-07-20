package org.apache.flink.runtime.hack.partition;

import org.apache.flink.runtime.checkpoint.channel.ResultSubpartitionInfo;
import org.apache.flink.runtime.hack.HackStringUtil;
import org.apache.flink.runtime.io.network.partition.BufferAvailabilityListener;
import org.apache.flink.runtime.io.network.partition.PipelinedSubpartition;
import org.apache.flink.runtime.io.network.partition.PipelinedSubpartitionView;
import org.apache.flink.runtime.io.network.partition.ResultPartition;

/**
 * A watcher class for {@link org.apache.flink.runtime.io.network.partition.PipelinedSubpartition}
 * and {@link org.apache.flink.runtime.io.network.partition.BoundedBlockingSubpartition}.
 */
public class HackSubpartitionWatcher {
	public static void printPipelinedSubpartitionFlush(PipelinedSubpartition subpartition, PipelinedSubpartitionView dataView) {
		ResultSubpartitionInfo resultSubpartitionInfo = subpartition.getSubpartitionInfo();
		BufferAvailabilityListener availabilityListener;
		ResultPartition parentPartition = subpartition.getParent();

		if (dataView != null) {
			availabilityListener = dataView.getAvailabilityListener();
			System.out.println("Subpartition info [" + resultSubpartitionInfo +
				"], " + "parent ResultPartition info [" + HackStringUtil.convertResultPartitionToString(parentPartition) +
				"], flush and notify data available to data viewer [" + dataView +
				"], and trigger BufferAvailableListener [" + HackStringUtil.convertBufferAvailabilityListenerToString(availabilityListener) + "]");
		} else {
			System.out.println("Subpartition info [" + resultSubpartitionInfo +
				"], " + "flush and notify data available to data viewer [PipelinedSubpartitionView is NULL]");
		}
	}
}

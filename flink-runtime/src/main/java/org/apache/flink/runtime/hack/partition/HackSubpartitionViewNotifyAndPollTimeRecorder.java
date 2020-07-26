package org.apache.flink.runtime.hack.partition;

import org.apache.flink.runtime.io.network.partition.PipelinedSubpartitionView;

/**
 * This class is for recording timestamps of data available and data poll.
 */
public class HackSubpartitionViewNotifyAndPollTimeRecorder {
	private static long notifyTimestamp = -1;

	public static void tickSubpartitionDataAvailable(PipelinedSubpartitionView subpartitionView) {
		notifyTimestamp = System.currentTimeMillis();
	}

	public static void tickSubpartitionDataPoll(PipelinedSubpartitionView subpartitionView) {
		if (notifyTimestamp == -1) {
			System.out.println("[ERROR!!!] Have not notified data available");
			return;
		}

		long pollTimestamp = System.currentTimeMillis();
		// TODO: have bugs, need to trace the (notify, poll) timestamp for each subpartitionView
//		System.out.println("PipelineSubpartitionView [" + HackStringUtil.convertPipelinedSubpartitionViewToString(subpartitionView) +
//			"] notify and poll delay is [" + (pollTimestamp - notifyTimestamp) + "] ms");
	}
}

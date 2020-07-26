package org.apache.flink.runtime.hack.partition;

import org.apache.flink.runtime.hack.HackStringUtil;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate;

/**
 * This is class is for monitoring the inputChannelWithData queue in
 * {@link org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate}.
 */
public class HackInputGateChannelQueueWatcher {
	public static void dumpLengthOfInputChannelWithData(SingleInputGate inputGate) {
		System.out.println("SingleInputGate [" + HackStringUtil.convertInputGateToString(inputGate) +
			"] queue of inputChannelWithData is [" + inputGate.getNumberOfInputChannelWithData());
	}
}

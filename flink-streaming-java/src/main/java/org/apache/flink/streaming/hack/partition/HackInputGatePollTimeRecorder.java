package org.apache.flink.streaming.hack.partition;

import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate;
import org.apache.flink.runtime.taskmanager.InputGateWithMetrics;
import org.apache.flink.streaming.runtime.io.CheckpointedInputGate;

import java.util.HashMap;
import java.util.Map;

/**
 * This is a class for recording the timestamps of polling data from InputGate.
 */
public class HackInputGatePollTimeRecorder {
	private static Map<String, Long> streamTaskNameToLastPollDataTimestamps = new HashMap<>();

	private static Object lock = new Object();

	private static long preProcessElementTimestamp;

	public static void tickPollDataFromInputGate(CheckpointedInputGate checkpointedInputGate) {
		InputGate inputGate = checkpointedInputGate.getInputGate();

		if (inputGate instanceof InputGateWithMetrics) {
			InputGateWithMetrics inputGateWithMetrics = (InputGateWithMetrics) inputGate;
			InputGate inputGate1 = inputGateWithMetrics.getInputGate();
			if (inputGate1 instanceof SingleInputGate) {
				SingleInputGate singleInputGate = (SingleInputGate) inputGate1;
				String taskName = singleInputGate.getOwningTaskName();

				synchronized (lock) {
					if (streamTaskNameToLastPollDataTimestamps.containsKey(taskName)) {
						long lastPollTimeStamp = streamTaskNameToLastPollDataTimestamps.get(taskName);
						System.out.println("Task [" + taskName +
							"] inputGate poll data interval [" + (System.currentTimeMillis() - lastPollTimeStamp) +
							"] ms");
					}
					streamTaskNameToLastPollDataTimestamps.put(taskName, System.currentTimeMillis());
				}
			} else {
				System.out.println("[ERROR!!!] IndexedInputGate [" + inputGate +
					"] is not SingleInputGate");
			}
		} else {
			System.out.println("[ERROR!!!] inputGate [" + inputGate +
				"] is not InputGateWithMetrics");
		}
	}

	/**
	 * TODO: potential bug, there may be multiple StreamTasks, and processElement() is preempted by another
	 * StreamTask. The previous {@code preProcessElementTimestamp} is wrong.
	 */
	public static void tickBeforeProcessElement() {
//		preProcessElementTimestamp = System.currentTimeMillis();
	}

	public static void tickAfterProcessElement(CheckpointedInputGate checkpointedInputGate) {
//		String taskName = StreamingHackStringUtils.convertCheckpointedInputGateToTaskName(checkpointedInputGate);
//		System.out.println("Task [" + taskName + "] process an record cost [" + (System.currentTimeMillis() - preProcessElementTimestamp) +
//			"] ms");
	}
}

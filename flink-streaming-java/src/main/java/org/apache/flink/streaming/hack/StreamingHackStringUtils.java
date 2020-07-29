package org.apache.flink.streaming.hack;

import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate;
import org.apache.flink.runtime.taskmanager.InputGateWithMetrics;
import org.apache.flink.streaming.runtime.io.CheckpointedInputGate;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

/**
 * String utils for converting a class to some info:
 * 1. StreamRecord;
 */
public class StreamingHackStringUtils {
	public static String convertStreamRecordToString(StreamRecord record, boolean containValue) {
		if (containValue) {
			return record.toString();
		} else {
			return "Record @ " + (record.hasTimestamp() ? record.getTimestamp() : "(undef)");
		}
	}

	public static String convertCheckpointedInputGateToTaskName(CheckpointedInputGate checkpointedInputGate) {
		InputGate inputGate = checkpointedInputGate.getInputGate();

		if (inputGate instanceof InputGateWithMetrics) {
			InputGateWithMetrics inputGateWithMetrics = (InputGateWithMetrics) inputGate;
			InputGate inputGate1 = inputGateWithMetrics.getInputGate();
			if (inputGate1 instanceof SingleInputGate) {
				SingleInputGate singleInputGate = (SingleInputGate) inputGate1;
				String taskName = singleInputGate.getOwningTaskName();

				return taskName;
			} else {
				return "[ERROR!!!] IndexedInputGate [" + inputGate +
					"] is not SingleInputGate";
			}
		} else {
			return "[ERROR!!!] inputGate [" + inputGate +
				"] is not InputGateWithMetrics";
		}
	}
}

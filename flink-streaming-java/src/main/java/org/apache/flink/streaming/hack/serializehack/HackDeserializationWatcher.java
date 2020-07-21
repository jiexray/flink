package org.apache.flink.streaming.hack.serializehack;

import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.hack.StreamingHackStringUtils;
import org.apache.flink.streaming.runtime.io.StreamTaskNetworkInput;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;

/**
 * A watcher class for deserializer in {@link StreamTaskNetworkInput}.
 */
public class HackDeserializationWatcher {
	public static boolean containValue = false;
	public static boolean debugDeserializer = false;

	public static void printDeserializedRecord(StreamTaskNetworkInput networkInput, StreamRecord record) {
		int inputIndex = networkInput.getInputIndex();
		int lastChannel = networkInput.getLastChannel();

		if (debugDeserializer) {
			System.out.println("StreamTaskNetworkInput index [" + inputIndex +
			"], lastChannel [" + lastChannel +
			"], deserialize record [" + StreamingHackStringUtils.convertStreamRecordToString(record, containValue) + "]");
		}
	}

	public static void printDataOutputBackOneInputOperator(OneInputStreamOperator operator, StreamRecord record) {
		AbstractStreamOperator abstractStreamOperator;
		if (operator instanceof AbstractStreamOperator) {
			abstractStreamOperator = (AbstractStreamOperator) operator;
		} else {
			System.out.println("[ERROR] cannot cast operator to AbstractUdfStreamOperator");
			return;
		}
		StreamTask task = abstractStreamOperator.getContainingTask();

		if (debugDeserializer) {
			System.out.println("InputGate output record [" + StreamingHackStringUtils.convertStreamRecordToString(record, containValue) +
				"] to task [" + task + "]");
		}
	}
}

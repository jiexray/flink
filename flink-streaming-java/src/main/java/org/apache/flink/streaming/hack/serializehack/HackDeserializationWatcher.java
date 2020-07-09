package org.apache.flink.streaming.hack.serializehack;

import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.io.StreamTaskNetworkInput;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;

/**
 * A watcher class for deserializer in {@link StreamTaskNetworkInput}.
 */
public class HackDeserializationWatcher {
	public static void printDeserializedRecord(StreamTaskNetworkInput networkInput, StreamRecord record) {
		int inputIndex = networkInput.getInputIndex();
		int lastChannel = networkInput.getLastChannel();

		System.out.println("StreamTaskNetworkInput index [" + inputIndex +
		"], lastChannel [" + lastChannel +
		"], deserialize record [" + record + "]");
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

		System.out.println("InputGate output record [" + record +
			"] to task [" + task + "]");
	}
}

package org.apache.flink.streaming.hack.serializehack;

import org.apache.flink.streaming.runtime.io.StreamTaskNetworkInput;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

/**
 * A watcher class for deserializer in {@link StreamTaskNetworkInput}.
 */
public class HackDeserializationWatcher {
	public static void printDeserializedRecord(StreamTaskNetworkInput networkInput, StreamRecord record) {
		int inputIndex = networkInput.getInputIndex();
		int lastChannel = networkInput.getLastChannel();

		System.out.println("StreamTaskNetworkInput index " + inputIndex +
		", lastChannel " + lastChannel +
		", deserialize record " + record);
	}
}

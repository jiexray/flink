package org.apache.flink.streaming.hack.partition;

/**
 * This class is a watcher class for monitoring the availability of InputGate and RecordWriter.
 */
public class HackInputOutputAvailableWatcher {
	public static void notifyInputProcessorUnavailable(String taskName) {
		System.out.println("[Important] InputProcessor of Task [" + taskName + "] is not available");
	}

	public static void notifyRecordWriterUnavailable(String taskName) {
		System.out.println("[Important] RecordWriter of Task [" + taskName + "] is not available");
	}
}

package org.apache.flink.streaming.hack.partition;

import org.apache.flink.streaming.hack.StreamingHackConfig;

/**
 * This class is a watcher class for monitoring the availability of InputGate and RecordWriter.
 */
public class HackInputOutputAvailableWatcher {
	public static void notifyInputProcessorUnavailable(String taskName) {
		if (StreamingHackConfig.hackAll) {
			doNotifyInputProcessorUnavailable(taskName);
		}
	}

	private static void doNotifyInputProcessorUnavailable(String taskName) {
		System.out.println("[Important] InputProcessor of Task [" + taskName + "] is not available");
	}

	public static void notifyRecordWriterUnavailable(String taskName) {
		if (StreamingHackConfig.hackAll) {
			doNotifyRecordWriterUnavailable(taskName);
		}
	}

	private static void doNotifyRecordWriterUnavailable(String taskName) {
		System.out.println("[Important] RecordWriter of Task [" + taskName + "] is not available");
	}

	public static void notifyInputProcessorAvailable(String taskName) {
		System.out.println("[Important] InputProcessor of Task [" + taskName + "] is back to available");
	}

	public static void notifyRecordWriterAvailable(String taskName) {
		System.out.println("[Important] RecordWriter of Task [" + taskName + "] is back to available");
	}
}

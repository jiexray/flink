package org.apache.flink.streaming.hack;

import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

/**
 * String utils for converting a class to some info:
 * 1. StreamRecord;
 */
public class StreamingHackStringUtils {
	public static String convertStreamRecordToString(StreamRecord record, boolean containValue) {
		if(containValue) {
			return record.toString();
		} else {
			return "Record @ " + (record.hasTimestamp() ? record.getTimestamp() : "(undef)");
		}
	}
}

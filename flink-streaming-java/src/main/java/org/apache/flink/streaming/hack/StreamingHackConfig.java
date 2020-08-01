package org.apache.flink.streaming.hack;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HackOptions;

/**
 * Configurations of hacking in streaming.
 */
public class StreamingHackConfig {
	public static boolean hackAll;

	public static void initialize(Configuration configuration) {
		hackAll = configuration.getBoolean(HackOptions.HACK_ALL);
	}
}

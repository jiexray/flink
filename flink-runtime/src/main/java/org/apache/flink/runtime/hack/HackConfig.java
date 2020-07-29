package org.apache.flink.runtime.hack;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HackOptions;

/**
 * Configurations of hacking in runtime.
 */
public class HackConfig {
	public static boolean HACK_PARTITION_REQUEST;

	public static void initialize(Configuration configuration) {
		HACK_PARTITION_REQUEST = configuration.getBoolean(HackOptions.HACK_PARTITION_REQUEST);
	}
}

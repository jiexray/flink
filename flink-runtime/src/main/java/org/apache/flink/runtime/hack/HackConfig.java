package org.apache.flink.runtime.hack;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HackOptions;

/**
 * Configurations of hacking in runtime.
 */
public class HackConfig {
	public static boolean hackPartitionRequest;

	public static void initialize(Configuration configuration) {
		hackPartitionRequest = configuration.getBoolean(HackOptions.HACK_PARTITION_REQUEST);
	}
}

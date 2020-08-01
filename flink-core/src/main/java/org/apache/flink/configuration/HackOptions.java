package org.apache.flink.configuration;

import static org.apache.flink.configuration.ConfigOptions.key;

/**
 * The set of configuration options relating to hacking.
 */
public class HackOptions {
	public static final ConfigOption<Boolean> HACK_PARTITION_REQUEST =
		key("hack.partition-request")
		.defaultValue(false)
		.withDescription("Switch on/off hacking on partition request");

	public static final ConfigOption<Boolean> HACK_ALL =
		key("hack.all")
		.defaultValue(false)
		.withDescription("Switch off all hacking parts");

}

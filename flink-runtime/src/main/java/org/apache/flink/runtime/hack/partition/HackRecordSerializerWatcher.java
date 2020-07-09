package org.apache.flink.runtime.hack.partition;

import org.apache.flink.runtime.io.network.api.serialization.SpanningRecordSerializer;

import java.nio.ByteBuffer;

/**
 * A watcher class for {@link SpanningRecordSerializer}.
 */
public class HackRecordSerializerWatcher {
	public static void printCopyToBufferBuilder(ByteBuffer dataBuffer) {
		System.out.println("Copy buffer len [" + dataBuffer.position() +
		"] to BufferBuilder");
	}
}

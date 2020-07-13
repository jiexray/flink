package org.apache.flink.runtime.hack.partition;

import org.apache.flink.runtime.io.network.api.serialization.SpanningRecordSerializer;
import org.apache.flink.runtime.io.network.api.writer.RecordWriter;

import java.nio.ByteBuffer;

/**
 * A watcher class for {@link SpanningRecordSerializer} and {@link RecordWriter}, basic for
 * high level serialization logical functions.
 */
public class HackRecordSerializerWatcher {
	public static void printCopyToBufferBuilder(ByteBuffer dataBuffer) {
//		System.out.println("Copy buffer len [" + dataBuffer.remaining() +
//		"] to BufferBuilder");
		return;
	}
}

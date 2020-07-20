package org.apache.flink.streaming.hack.serializehack;

import org.apache.flink.runtime.io.network.api.serialization.SpanningRecordSerializer;
import org.apache.flink.runtime.io.network.api.writer.RecordWriter;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.io.network.partition.ResultPartition;
import org.apache.flink.streaming.hack.StreamingHackStringUtils;
import org.apache.flink.streaming.runtime.io.RecordWriterOutput;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.OutputTag;

/**
 * A watcher class for {@link SpanningRecordSerializer}.
 */
public class HackSerializationWatcher {
	private static boolean containValue = false;

	public static void printSerializedRecord(RecordWriterOutput recordWriterOutput, StreamRecord record) {
		OutputTag outputTag = recordWriterOutput.getOutputTag();
		RecordWriter recordWriter = recordWriterOutput.getRecordWriter();
		ResultPartitionWriter resultPartitionWriter = recordWriter.getTargetPartition();
		ResultPartition resultPartition;

		if (resultPartitionWriter instanceof ResultPartition) {
			resultPartition = (ResultPartition) resultPartitionWriter;
		} else {
			System.out.println("[ERROR] resultPartitionWriter is not a ResultPartition");
			return;
		}

		String owningTask = resultPartition.getOwningTaskName();

		System.out.println("ResultWriterOutput with OutputTag [" + (outputTag == null ? "NULL" : outputTag) +
		"], send to ResultParition [" + resultPartition +
		"], task [" + owningTask +
		"], record [" + StreamingHackStringUtils.convertStreamRecordToString(record, containValue) + "]");
	}
}

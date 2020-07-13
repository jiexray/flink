package org.apache.flink.runtime.hack.partition;

import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.BufferPoolOwner;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.partition.ResultPartition;

/**
 * A watcher class for {@link BufferPool}, typically, {@link NetworkBufferPool}.
 */
public class HackRequestBufferBuilderWatcher {
	public static void printRequestBufferBuilder(int targetChannel, BufferPoolOwner owner) {
		return;
//		System.out.println("BufferOwner (ResultPartition) [" + convertBufferOwnerToString(owner) +
//		"] request buffer on channel [" + targetChannel + "]");
	}

	public static void printRequestFromGlobal(BufferPoolOwner owner) {
		return;
//		System.out.println("BufferOwner (ResultPartition) [" + convertBufferOwnerToString(owner) +
//		"] request buffer from global (NetworkBufferPool)");
	}

	private static String convertBufferOwnerToString(BufferPoolOwner owner) {
		if (owner == null) {
			return "BufferPoolOwner is NULL";
		}
		ResultPartition resultPartition;

		if (owner instanceof ResultPartition) {
			resultPartition = (ResultPartition) owner;
		} else {
			System.out.println("[ERROR] Cannot cast BufferPoolOwner to ResultPartition");
			return "";
		}

		return convertResultPartitionToString(resultPartition);
	}

	private static String convertResultPartitionToString(ResultPartition resultPartition) {
		return resultPartition +
			", owning task [" + resultPartition.getOwningTaskName() +
			"]";
	}
}

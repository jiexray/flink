package org.apache.flink.runtime.hack.partition;

import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.BufferPoolOwner;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;

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
}

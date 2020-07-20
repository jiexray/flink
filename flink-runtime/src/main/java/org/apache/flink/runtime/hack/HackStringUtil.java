package org.apache.flink.runtime.hack;

import org.apache.flink.runtime.io.network.buffer.BufferPoolOwner;
import org.apache.flink.runtime.io.network.netty.HackCreditBasedBufferAvailabilityListenerWatcher;
import org.apache.flink.runtime.io.network.partition.BufferAvailabilityListener;
import org.apache.flink.runtime.io.network.partition.ResultPartition;
import org.apache.flink.runtime.io.network.partition.consumer.LocalInputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.RemoteInputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate;

/**
 * Convert Class to info strings.
 */
public class HackStringUtil {
	public static String convertRemoteInputChannelToString(RemoteInputChannel inputChannel) {
		SingleInputGate inputGate = inputChannel.getInputGate();

		return "RemoteInputChannel [" + inputChannel +
			"], for inputGate [" + convertInputGateToString(inputGate) + "]";
	}

	public static String convertInputGateToString (SingleInputGate inputGate) {
		String owningTask = inputGate.getOwningTaskName();
		return "SingleInputGate for task [" + owningTask + "]";
	}

	public static String convertBufferOwnerToString(BufferPoolOwner owner) {
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

	public static String convertBufferAvailabilityListenerToString(BufferAvailabilityListener availabilityListener) {
		if (availabilityListener == null) {
			return "BufferAvailabilityListener is NULL";
		} else if (availabilityListener instanceof LocalInputChannel) {
			LocalInputChannel localInputChannel = (LocalInputChannel) availabilityListener;
			return localInputChannel.toString();
		} else {
			return HackCreditBasedBufferAvailabilityListenerWatcher.printCreditBasedSequenceNumberingViewReader(availabilityListener);
		}
	}

	public static String convertResultPartitionToString(ResultPartition partition) {
		String owningTask = partition.getOwningTaskName();

		return "owning task [" + owningTask + "], partition info [" + partition + "]";
	}
}

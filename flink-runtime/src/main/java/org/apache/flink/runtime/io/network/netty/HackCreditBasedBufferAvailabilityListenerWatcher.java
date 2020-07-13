package org.apache.flink.runtime.io.network.netty;

import org.apache.flink.runtime.io.network.partition.BufferAvailabilityListener;

/**
 * A watcher class for {@link org.apache.flink.runtime.io.network.netty.CreditBasedSequenceNumberingViewReader}.
 */
public class HackCreditBasedBufferAvailabilityListenerWatcher {
	public static String printCreditBasedSequenceNumberingViewReader(BufferAvailabilityListener availabilityListener) {
		CreditBasedSequenceNumberingViewReader viewReader;
		if (availabilityListener instanceof CreditBasedSequenceNumberingViewReader) {
			viewReader = (CreditBasedSequenceNumberingViewReader) availabilityListener;
		} else {
			return "BufferAvailabilityListener is not CreditBasedSequenceNumberingViewReader";
		}

		return viewReader.toString();
	}
}

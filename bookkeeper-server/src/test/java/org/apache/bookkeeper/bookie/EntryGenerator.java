package org.apache.bookkeeper.bookie;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

public class EntryGenerator {

	public static ByteBuf create(long ledger, long entry) {
        byte[] data = generateDataString(ledger, entry).getBytes();
        ByteBuf bb = Unpooled.buffer(8 + 8 + data.length);
        bb.writeLong(ledger);
        bb.writeLong(entry);
        bb.writeBytes(data);
        return bb;
    }
    
	public static ByteBuf create(String text) {
        byte[] data = text.getBytes();
        ByteBuf bb = Unpooled.buffer(data.length);
        bb.writeBytes(data);
        return bb;
    }
    
	public static String generateDataString(long ledgerId, long entryId) {
        return ("TEST[" + ledgerId + "," + entryId + "]");
    }
}

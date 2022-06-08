package org.apache.bookkeeper.bookie;

import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;

import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.util.DiskChecker;
import org.apache.bookkeeper.util.IOUtils;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;

@RunWith (value=Parameterized.class)
public class DefaultEntryLoggerTest {
	
	enum Type {ADD_ENTRY, READ_ENTRY, EXTRACT_METADATA};

	BookieImpl bookie;

	// used by all tests
	private DefaultEntryLogger entryLogger;
	private File ledgerDir;
	private Type type;
	private boolean validParam;
	
	// common test parameters
	private String expected;
	private long ledgerId;

	// addEntryTest parameters
	private ByteBuffer bb;
	
	// readEntryTest parameters
	private long entryId;
	private Long entryLocation;
	private boolean corruptedLog;
	private boolean corruptedSize;
	// helpful
	private long realLocation = -1;
	
	// extractEntryLogMetadataFromIndexTest parameters
	private long entryLogId;
	private Long hdrVersion;
	private Long lmLedgerId;
	private Long lmEntryId;
	private boolean lmSubLedgers;
	private boolean lmOutOfBound;
	private boolean lmMaxOffset;
	// helpful
	private long realEntryLogId;
	
	// utility functions
	private static long L(long l) {
		return Long.valueOf(l);
	}
	
	private static boolean B(boolean b) {
		return Boolean.valueOf(b);
	}

	@Parameters
	public static Collection<Object[]> data() {
		return Arrays.asList(new Object[][] {
			/* README: all columns named as 'null' are not considered parameters for the corresponding test */

			// type, expected, ledgerId, entry, valid param, null, null, null, null, null
			{Type.ADD_ENTRY, "Keys and values must be >= 0", L(-1), EntryGenerator.create(-1, 0), B(true), null, null, null, null, null},
			{Type.ADD_ENTRY, "TEST[0,0]", L(0), EntryGenerator.create(0, 0), B(true), null, null, null, null, null},
			{Type.ADD_ENTRY, null, L(1), null, B(false), null, null, null, null, null},
			{Type.ADD_ENTRY, "Invalid entry", L(1), EntryGenerator.create("Invalid entry"), B(false), null, null, null, null, null},
			// type, expected, ledgerId, null, corruptedLog, entryId, entry location, null, corruptedSize, null
			{Type.READ_ENTRY, null, L(-1), null, B(false), L(0), L(-1), null, B(false), null},
			{Type.READ_ENTRY, "TEST[0,0]", L(0), null, B(false), L(0), null, null, B(false), null},
			{Type.READ_ENTRY, null, L(0), null, B(false), L(-1), L(0), null, B(false), null},
			{Type.READ_ENTRY, null, L(1), null, B(false), L(1), L(1), null, B(false), null},
			{Type.READ_ENTRY, "Short read from entrylog 0", L(0), null, B(true), L(0), null, null, B(false), null},
			{Type.READ_ENTRY, "Short read for 0@0 in 0@1028(0!=25)", L(0), null, B(false), L(0), null, null, B(true), null},
			// type, expected, hdrVersion, null, lmSubLedgers, lmLedgerId, lmEntryId, entryLogId, lmOutOfBound, lmMaxOffset
			{Type.EXTRACT_METADATA, null, null, null, B(false), null, null, L(-1), B(false), B(false)},
			{Type.EXTRACT_METADATA, "{ totalSize = 58, remainingSize = 58, ledgersMap = ConcurrentLongLongHashMap{0 => 58} }",
					null, null, B(false), null, null, L(0), B(false), B(false)},
			{Type.EXTRACT_METADATA, null, null, null, B(false), null, null, L(1), B(false), B(false)},
			{Type.EXTRACT_METADATA, "Old log file header without ledgers map on entryLogId 0", L(0), null, B(false), null, null, L(0), B(false), B(false)},
			{Type.EXTRACT_METADATA, "Cannot deserialize ledgers map from ledger 0", null, null, B(false), L(0), null, L(0), B(false), B(false)},
			{Type.EXTRACT_METADATA, "Cannot deserialize ledgers map from entryId 0", null, null, B(false), null, L(0), L(0), B(false), B(false)},
			{Type.EXTRACT_METADATA, "Invalid entry size when reading ledgers map", null, null, B(true), null, null, L(0), B(false), B(false)},
			{Type.EXTRACT_METADATA, null, null, null, B(false), null, null, L(0), B(true), B(false)},
			{Type.EXTRACT_METADATA, "Not all ledgers were found in ledgers map index. expected: 1 -- found: 0 -- entryLogId: 0",
					null, null, B(false), null, null, L(0), B(false), B(true)},
		});
	}
	
	public DefaultEntryLoggerTest(Type type, String expected, Long longParam1, ByteBuf bb, Boolean boolParam1,
			Long longParam2, Long longParam3, Long longParam4, Boolean boolParam2, Boolean boolParam3) throws Exception {
		if (type == Type.ADD_ENTRY)
			configure(type, expected, longParam1, bb, boolParam1);
		else if (type == Type.READ_ENTRY)
			configure(type, expected, longParam1, boolParam1, longParam2, longParam3, boolParam2);
		else
			configure(type, expected, longParam1, boolParam1, longParam4, longParam2, 
					longParam3, boolParam2, boolParam3);
	}

	public void configure(Type type, String expected, Long ledgerId,
						  ByteBuf bb, Boolean validParam) throws Exception {
		this.type = type;
		this.expected = expected;
		this.ledgerId = ledgerId.longValue();
		this.bb = (bb == null) ? null : bb.nioBuffer();
		this.validParam = validParam.booleanValue();
		
		ServerConfiguration conf = new ServerConfiguration();
		prepareEnv(conf);
		// instance the class under test
		entryLogger = new DefaultEntryLogger(conf, bookie.getLedgerDirsManager());
	}
	
	public void configure(Type type, String expected, Long ledgerId, Boolean corruptedLog,
			Long entryId, Long entryLocation, Boolean corruptedSize) throws Exception {
		this.type = type;
		this.expected = expected;
		this.ledgerId = ledgerId.longValue();
		this.corruptedLog = corruptedLog.booleanValue();
		this.entryId = entryId.longValue();
		this.entryLocation = entryLocation;
		this.corruptedSize = corruptedSize.booleanValue();
		
		ByteBufAllocator allocator;
		ServerConfiguration conf = new ServerConfiguration();
		prepareEnv(conf);
		// instance the class under test
		if (this.corruptedSize) {
			allocator = Mockito.spy(PooledByteBufAllocator.DEFAULT);
			Mockito.doAnswer(invocation -> {
				return Unpooled.buffer(0, 0);
			}).when(allocator).buffer(Mockito.anyInt(), Mockito.anyInt());
		} else {
			allocator = PooledByteBufAllocator.DEFAULT;
		}
		entryLogger = new DefaultEntryLogger(conf, bookie.getLedgerDirsManager(), null, NullStatsLogger.INSTANCE, allocator);
		// avoid addEntry method exceptions
		ledgerId = (ledgerId < 0) ? -ledgerId : ledgerId;
		entryId = (entryId < 0) ? -entryId : entryId;
		// add entry
		ByteBuffer bb = EntryGenerator.create(ledgerId.longValue(), entryId.longValue()).nioBuffer();
		realLocation = entryLogger.addEntry(ledgerId.longValue(), bb);
		entryLogger.flush();
	}
	
	private void configure(Type type, String expected, Long hdrVersion, boolean lmSubLedgers, Long entryLogId,
			Long lmLedgerId, Long lmEntryId, boolean lmOutOfBound, boolean lmMaxOffset) throws Exception {
		this.type = type;
		this.expected = expected;
		this.lmSubLedgers = lmSubLedgers;
		this.entryLogId = entryLogId.longValue();
		this.lmOutOfBound = lmOutOfBound;
		this.lmMaxOffset = lmMaxOffset;

		ServerConfiguration conf = new ServerConfiguration();
		prepareEnv(conf);
		// instance the class under test
		entryLogger = new DefaultEntryLogger(conf, bookie.getLedgerDirsManager());
		// add entry "TEST[0,0]"
		realLocation = entryLogger.addEntry(0, EntryGenerator.create(0, 0).nioBuffer());
		// add entry "TEST[0,1]"
		realLocation = entryLogger.addEntry(0, EntryGenerator.create(0, 1).nioBuffer());
		entryLogger.flush();
		// create log
		EntryLogManagerBase entryLogManager = (EntryLogManagerBase) entryLogger.getEntryLogManager();
        entryLogManager.createNewLog(0);
        entryLogManager.flush();
        realEntryLogId = DefaultEntryLogger.logIdForOffset(realLocation);
        
        if (hdrVersion != null) {
        	this.hdrVersion = hdrVersion;
        	setHeaderVersion();
        }
        
        long offset = (getLedgerMapOffset());
        
        if (lmLedgerId != null) {
        	this.lmLedgerId = lmLedgerId;
        	setLedgerIdInLedgerMap(offset);
        }
        
        if (lmEntryId != null) {
        	this.lmEntryId = lmEntryId;
        	setEntryIdInLedgerMap(offset);
        }
        
        if (this.lmSubLedgers)
        	subtractLedgersInLedgerMap(offset);
        
        if (this.lmOutOfBound || this.lmMaxOffset)
        	corruptLedgerMapOffset(offset, this.lmMaxOffset);
	}
	
	private void corruptLedgerMapOffset(long offset, boolean maxSize) throws FileNotFoundException, IOException {
		File fileLog = new File(ledgerDir.getAbsolutePath() + "/current/0.log");
        RandomAccessFile raf = new RandomAccessFile(fileLog, "rw");
        try {
            raf.seek(offset);
            long newOffset = (maxSize) ? Long.MAX_VALUE : offset + raf.readInt();
            raf.seek(DefaultEntryLogger.LEDGERS_MAP_OFFSET_POSITION);
            raf.writeLong(newOffset);
        } finally {
            raf.close();
        }
	}
	
	private void subtractLedgersInLedgerMap(long offset) throws FileNotFoundException, IOException {
		File fileLog = new File(ledgerDir.getAbsolutePath() + "/current/0.log");
        RandomAccessFile raf = new RandomAccessFile(fileLog, "rw");
        try {
            raf.seek(offset + 4 + 8 + 8);
            int ledgersNum = raf.readInt();
            raf.seek(offset + 4 + 8 + 8);
            raf.writeInt(ledgersNum - 1);
        } finally {
            raf.close();
        }
	}
	
	private void setEntryIdInLedgerMap(long offset) throws FileNotFoundException, IOException {
		File fileLog = new File(ledgerDir.getAbsolutePath() + "/current/0.log");
        RandomAccessFile raf = new RandomAccessFile(fileLog, "rw");
        try {
            raf.seek(offset + 4 + 8);
            raf.writeLong(lmEntryId);
        } finally {
            raf.close();
        }
	}
	
	private void setLedgerIdInLedgerMap(long offset) throws FileNotFoundException, IOException {
		File fileLog = new File(ledgerDir.getAbsolutePath() + "/current/0.log");
        RandomAccessFile raf = new RandomAccessFile(fileLog, "rw");
        try {
            raf.seek(offset + 4);
            raf.writeLong(lmLedgerId);
        } finally {
            raf.close();
        }
	}
	
	private long getLedgerMapOffset() throws FileNotFoundException, IOException {
		File fileLog = new File(ledgerDir.getAbsolutePath() + "/current/0.log");
        RandomAccessFile raf = new RandomAccessFile(fileLog, "rw");
        try {
            raf.seek(DefaultEntryLogger.LEDGERS_MAP_OFFSET_POSITION);
            return raf.readLong();
        } finally {
            raf.close();
        }
	}
	
	private void setHeaderVersion() throws FileNotFoundException, IOException {
		File fileLog = new File(ledgerDir.getAbsolutePath() + "/current/0.log");
        RandomAccessFile raf = new RandomAccessFile(fileLog, "rw");
        try {
            raf.seek(DefaultEntryLogger.HEADER_VERSION_POSITION);
            raf.writeLong(this.hdrVersion.longValue());
        } finally {
            raf.close();
        }
	}
	
	private void prepareEnv(ServerConfiguration conf) throws IOException {
		ledgerDir = IOUtils.createTempDir("bkTest", ".dir");
        conf.setLedgerStorageClass(InterleavedLedgerStorage.class.getName());
        conf.setJournalDirName(ledgerDir.toString());
        conf.setLedgerDirNames(new String[] { ledgerDir.getAbsolutePath() });
        conf.setAdvertisedAddress("127.0.0.1");

        bookie = mock(BookieImpl.class);
		when(bookie.getLedgerDirsManager()).thenAnswer(new Answer<LedgerDirsManager>() {
			@Override
			public LedgerDirsManager answer(InvocationOnMock unused) throws Throwable {
		        DiskChecker diskChecker = BookieResources.createDiskChecker(conf);
		        LedgerDirsManager ledgerDirsManager = BookieResources.createLedgerDirsManager(
		                conf, diskChecker, NullStatsLogger.INSTANCE);
		        
		        return ledgerDirsManager;
			}
		});
	}

	@Test
	public void addEntryTest() throws IOException {
		assumeTrue(type == Type.ADD_ENTRY);
		// add entry
		if (ledgerId < 0) {
			Exception e = assertThrows(IllegalArgumentException.class,
									   () -> entryLogger.addEntry(ledgerId, bb));
			assertEquals(expected, e.getMessage());
		} else if (bb == null) {
			assertThrows(NullPointerException.class,
						 () -> entryLogger.addEntry(ledgerId, bb));
		} else {
			entryLogger.addEntry(ledgerId, bb);
			entryLogger.flush();
			// verify written entry
			File fileLog = new File(ledgerDir.getAbsolutePath() + "/current/0.log");
			BufferedReader br = new BufferedReader(new FileReader(fileLog));
			String ledgerContent = br.readLine();
			br.close();
			if (validParam) {
				assertTrue(ledgerContent.contains(expected));
			} else {
				assertFalse(ledgerContent.contains(expected));
			}
		}
	}
	
	@Test
	public void readEntryTest() throws IOException {
		assumeTrue(type == Type.READ_ENTRY);
		if (ledgerId < 0 || entryId < 0) {
			assertThrows(IOException.class,
					     () -> entryLogger.readEntry(ledgerId, entryId, realLocation));
		} else if (entryLocation != null && entryLocation.longValue() != realLocation) {
			long location = entryLocation.longValue();
			if (location < 0)
				assertThrows(IOException.class,
						 	 () -> entryLogger.readEntry(ledgerId, entryId, location));
			else
				assertThrows(IllegalArgumentException.class,
							 () -> entryLogger.readEntry(ledgerId, entryId, location));
		} else if (corruptedLog) {
			File fileLog = new File(ledgerDir.getAbsolutePath() + "/current/0.log");
			BufferedWriter bw = new BufferedWriter(new FileWriter(fileLog));
			bw.append("corrupt");
			bw.flush();
			bw.close();
			Exception e = assertThrows(Bookie.NoEntryException.class,
					 				   () -> entryLogger.readEntry(ledgerId, entryId, realLocation));
			assertEquals(expected, e.getMessage());
		} else if (corruptedSize) {
			Exception e = assertThrows(Bookie.NoEntryException.class,
	 				   				   () -> entryLogger.readEntry(ledgerId, entryId, realLocation));
			assertEquals(expected, e.getMessage());
		} else {	
			ByteBuf retrievedEntry = entryLogger.readEntry(ledgerId, entryId, realLocation);
		    assertEquals(ledgerId, retrievedEntry.readLong());
		    assertEquals(entryId, retrievedEntry.readLong());
		    byte[] data = new byte[retrievedEntry.readableBytes()];
		    retrievedEntry.readBytes(data);
		    retrievedEntry.release();
		    assertEquals(expected, new String(data));
		}
	}

	@Test
    public void extractEntryLogMetadataFromIndexTest() throws IOException {
		assumeTrue(type == Type.EXTRACT_METADATA);
		if (lmOutOfBound) {
			assertThrows(IOException.class,
					     () -> entryLogger.extractEntryLogMetadataFromIndex(realEntryLogId));
		} else if (hdrVersion != null || lmLedgerId != null || lmEntryId != null || lmSubLedgers || lmMaxOffset) {
			Exception e = assertThrows(IOException.class,
									   () -> entryLogger.extractEntryLogMetadataFromIndex(realEntryLogId));
			assertEquals(expected, e.getMessage());
		} else if (entryLogId == realEntryLogId) { // in configure() we have created a new log with id 0L
	        EntryLogMetadata entryLogMeta =  entryLogger.extractEntryLogMetadataFromIndex(entryLogId);
	        assertEquals(expected, entryLogMeta.toString());
	    // when we add an entry the files '0.log' and '1.log' are created
		} else if (entryLogId == realEntryLogId + 1) {
			assertThrows(IOException.class,
				     	 () -> entryLogger.extractEntryLogMetadataFromIndex(entryLogId));
		} else {
			assertThrows(FileNotFoundException.class,
			     	 	 () -> entryLogger.extractEntryLogMetadataFromIndex(entryLogId));
		}
    }
	
	@After
    public void cleanUp() throws Exception {
		try {
			if (null != this.entryLogger) {
	            entryLogger.close();
	        }
	        FileUtils.deleteDirectory(ledgerDir);
		} catch(Exception e) {
			// do nothing
		}
	}

}
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
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
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
	// helpful
	private long realEntryLogId;

	@Parameters
	public static Collection<Object[]> data() {
		return Arrays.asList(new Object[][] {
			/* README: all columns named as 'null' are not considered parameters for the corresponding test */

			// type, expected, ledgerId, entry, valid param, null, null, null, null
			{Type.ADD_ENTRY, "Keys and values must be >= 0", Long.valueOf(-1), EntryGenerator.create(-1, 0), Boolean.valueOf(true), null, null, null, null},
			{Type.ADD_ENTRY, "TEST[0,0]", Long.valueOf(0), EntryGenerator.create(0, 0), Boolean.valueOf(true), null, null, null, null},
			{Type.ADD_ENTRY, null, Long.valueOf(1), null, Boolean.valueOf(false), null, null, null, null},
			{Type.ADD_ENTRY, "Invalid entry", Long.valueOf(1), EntryGenerator.create("Invalid entry"), Boolean.valueOf(false), null, null, null, null},
			// type, expected, ledgerId, null, corruptedLog, entryId, entry location, null, corruptedSize
			{Type.READ_ENTRY, null, Long.valueOf(-1), null, Boolean.valueOf(false), Long.valueOf(0), Long.valueOf(-1), null, Boolean.valueOf(false)},
			{Type.READ_ENTRY, "TEST[0,0]", Long.valueOf(0), null, Boolean.valueOf(false), Long.valueOf(0), null, null, Boolean.valueOf(false)},
			{Type.READ_ENTRY, null, Long.valueOf(0), null, Boolean.valueOf(false), Long.valueOf(-1), Long.valueOf(0), null, Boolean.valueOf(false)},
			{Type.READ_ENTRY, null, Long.valueOf(1), null, Boolean.valueOf(false), Long.valueOf(1), Long.valueOf(1), null, Boolean.valueOf(false)},
			{Type.READ_ENTRY, "Short read from entrylog 0", Long.valueOf(0), null, Boolean.valueOf(true), Long.valueOf(0), null, null, Boolean.valueOf(false)},
			{Type.READ_ENTRY, "Short read for 0@0 in 0@1028(0!=25)", Long.valueOf(0), null, Boolean.valueOf(false), Long.valueOf(0), null, null, Boolean.valueOf(true)},
			// type, expected, null, null, null, null, null, entryLogId, null
			{Type.EXTRACT_METADATA, null, null, null, null, null, null, Long.valueOf(-1), null},
			{Type.EXTRACT_METADATA, "{ totalSize = 29, remainingSize = 29, ledgersMap = ConcurrentLongLongHashMap{0 => 29} }",
					null, null, null, null, null, Long.valueOf(0), null},
			{Type.EXTRACT_METADATA, null, null, null, null, null, null, Long.valueOf(1), null},
		});
	}
	
	public DefaultEntryLoggerTest(Type type, String expected, Long ledgerId, ByteBuf bb, 
			Boolean boolParam1, Long entryId, Long entryLocation, Long entryLogId, Boolean boolParam2) throws Exception {
		if (type == Type.ADD_ENTRY)
			configure(type, expected, ledgerId, bb, boolParam1);
		else if (type == Type.READ_ENTRY)
			configure(type, expected, ledgerId, boolParam1, entryId, entryLocation, boolParam2);
		else
			configure(type, expected, entryLogId);
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
	
	private void configure(Type type, String expected, Long entryLogId) throws Exception {
		this.type = type;
		this.expected = expected;
		this.entryLogId = entryLogId.longValue();
		

		ServerConfiguration conf = new ServerConfiguration();
		prepareEnv(conf);
		// instance the class under test
		entryLogger = new DefaultEntryLogger(conf, bookie.getLedgerDirsManager());
		// add entry "TEST[0,0]"
		realLocation = entryLogger.addEntry(0, EntryGenerator.create(0, 0).nioBuffer());
		entryLogger.flush();
		// create log
		EntryLogManagerBase entryLogManager = (EntryLogManagerBase) entryLogger.getEntryLogManager();
        entryLogManager.createNewLog(0);
        entryLogManager.flush();
        realEntryLogId = DefaultEntryLogger.logIdForOffset(realLocation);
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
		// in configure() we create a new log with id 0L
		if (entryLogId == realEntryLogId) {
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
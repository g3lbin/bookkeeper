package org.apache.bookkeeper.bookie;

import static org.apache.bookkeeper.bookie.BookKeeperServerStats.BOOKIE_SCOPE;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.LD_INDEX_SCOPE;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.LD_LEDGER_SCOPE;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.function.Supplier;

import org.apache.bookkeeper.common.allocator.ByteBufAllocatorWithOomHandler;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.discover.BookieServiceInfo;
import org.apache.bookkeeper.discover.RegistrationManager;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.server.conf.BookieConfiguration;
import org.apache.bookkeeper.server.service.StatsProviderService;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.util.DiskChecker;
import org.apache.bookkeeper.util.IOUtils;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

@RunWith (value=Parameterized.class)
public class DefaultEntryLoggerTest {
	
	enum Type {ADD_ENTRY, READ_ENTRY};

	@Rule public MockitoRule rule = MockitoJUnit.rule();

	@Mock
	Supplier<BookieServiceInfo> bookieServiceInfoProvider;
	@Mock
	RegistrationManager rm;
	@Mock
	LedgerManager ledgerManager;

	// used by all tests
	private DefaultEntryLogger entryLogger;
	private File ledgerDir;
	private Type type;
	
	// common test parameters
	private String expectedEntry;
	private long ledgerId;
	private boolean validParam;

	// addEntryTest parameters
	private ByteBuffer bb;
	
	// readEntryTest parameters
	private long entryId;
	private Long entryLocation;

	private long realLocation = -1;

	@Parameters
	public static Collection<Object[]> data() {
		return Arrays.asList(new Object[][] {
			// type, expected, ledgerId, entry, valid param, null, null
			{Type.ADD_ENTRY, "Keys and values must be >= 0", Long.valueOf(-1), generateEntry(-1, 0), true, null, null},
			{Type.ADD_ENTRY, "TEST[0,0]", Long.valueOf(0), generateEntry(0, 0), true, null, null},
			{Type.ADD_ENTRY, "TEST[1,0]", Long.valueOf(1), generateEntry(1, 0), true, null, null},
			{Type.ADD_ENTRY, null, Long.valueOf(1), null, false, null, null},
			{Type.ADD_ENTRY, "Invalid entry", Long.valueOf(1), generateInvalidParam("Invalid entry"), false, null, null},
			// type, expected, ledgerId, null, valid param, entryId, entry location
			{Type.READ_ENTRY, null, Long.valueOf(-1), null, true, Long.valueOf(0), null},
			{Type.READ_ENTRY, "TEST[0,0]", Long.valueOf(0), null, true, Long.valueOf(0), null},
			{Type.READ_ENTRY, null, Long.valueOf(0), null, true, Long.valueOf(-1), null},
			{Type.READ_ENTRY, null, Long.valueOf(0), null, true, Long.valueOf(0), Long.valueOf(0)},
			{Type.READ_ENTRY, null, Long.valueOf(0), null, true, Long.valueOf(0), Long.valueOf(1)},
			{Type.READ_ENTRY, null, Long.valueOf(0), null, true, Long.valueOf(0), Long.valueOf(-1)},
		});
	}
	
	public DefaultEntryLoggerTest(Type type, String expectedEntry, Long ledgerId, ByteBuffer bb,
								  boolean validParam, Long entryId, Long entryLocation) throws Exception {
		if (type == Type.ADD_ENTRY)
			configure(type, expectedEntry, ledgerId, bb, validParam);
		else
			configure(type, expectedEntry, ledgerId, validParam, entryId, entryLocation);
	}

	public void configure(Type type, String expectedEntry, Long ledgerId,
						  ByteBuffer bb, boolean validParam) throws Exception {
		this.type = type;
		this.expectedEntry = expectedEntry;
		this.ledgerId = ledgerId.longValue();
		this.bb = bb;
		this.validParam = validParam;
		
		ServerConfiguration conf = new ServerConfiguration();
		// build a bookie
		BookieImpl bookie = bookieBuilder(conf);
		// instance the class under test
		entryLogger = new DefaultEntryLogger(conf, bookie.getLedgerDirsManager());
	}
	
	public void configure(Type type, String expectedEntry, Long ledgerId, boolean validParam,
			  Long entryId, Long entryLocation) throws Exception {
		this.type = type;
		this.expectedEntry = expectedEntry;
		this.ledgerId = ledgerId.longValue();
		this.entryId = entryId.longValue();
		this.validParam = validParam;
		this.entryLocation = entryLocation;
		
		ServerConfiguration conf = new ServerConfiguration();
		// build a bookie
		BookieImpl bookie = bookieBuilder(conf);
		// instance the class under test
		entryLogger = new DefaultEntryLogger(conf, bookie.getLedgerDirsManager());
		if (validParam) {
			// add entry
			// avoid addEntry method exceptions
			ledgerId = (ledgerId < 0) ? -ledgerId : ledgerId;
			entryId = (entryId < 0) ? -entryId : entryId;
			ByteBuffer bb = generateEntry(ledgerId.longValue(), entryId.longValue());
			this.realLocation = entryLogger.addEntry(ledgerId.longValue(), bb);
			entryLogger.flush();
		}
	}

	private BookieImpl bookieBuilder(ServerConfiguration conf) throws Exception {
		ledgerDir = IOUtils.createTempDir("bkTest", ".dir");
        conf.setLedgerStorageClass(InterleavedLedgerStorage.class.getName());
        conf.setJournalDirName(ledgerDir.toString());
        conf.setLedgerDirNames(new String[] { ledgerDir.getAbsolutePath() });
        conf.setAdvertisedAddress("127.0.0.1");

        StatsProviderService statsProviderService = new StatsProviderService(new BookieConfiguration(conf));
        StatsLogger rootStatsLogger = statsProviderService.getStatsProvider().getStatsLogger("");
        StatsLogger bookieStats = rootStatsLogger.scope(BOOKIE_SCOPE);
        DiskChecker diskChecker = BookieResources.createDiskChecker(conf);
        LedgerDirsManager ledgerDirsManager = BookieResources.createLedgerDirsManager(
                conf, diskChecker, bookieStats.scope(LD_LEDGER_SCOPE));
        LedgerDirsManager indexDirsManager = BookieResources.createIndexDirsManager(
                conf, diskChecker, bookieStats.scope(LD_INDEX_SCOPE), ledgerDirsManager);

        ByteBufAllocatorWithOomHandler allocator = BookieResources.createAllocator(conf);
        
        LedgerStorage storage = BookieResources.createLedgerStorage(
                conf, ledgerManager, ledgerDirsManager, indexDirsManager, bookieStats, allocator);
        
        return new BookieImpl(conf, rm, storage, diskChecker, ledgerDirsManager, indexDirsManager,
						      bookieStats, allocator, bookieServiceInfoProvider);
	}

    private static ByteBuffer generateEntry(long ledger, long entry) {
        byte[] data = generateDataString(ledger, entry).getBytes();
        ByteBuf bb = Unpooled.buffer(8 + 8 + data.length);
        bb.writeLong(ledger);
        bb.writeLong(entry);
        bb.writeBytes(data);
        return bb.nioBuffer();
    }
    
    private static ByteBuffer generateInvalidParam(String text) {
        byte[] data = text.getBytes();
        ByteBuf bb = Unpooled.buffer(data.length);
        bb.writeBytes(data);
        return bb.nioBuffer();
    }
    
    private static String generateDataString(long ledgerId, long entryId) {
        return ("TEST[" + ledgerId + "," + entryId + "]");
    }

	@Test
	public void addEntryTest() throws IOException {
		assumeTrue(type == Type.ADD_ENTRY);
		// add entry
		if (ledgerId < 0) {
			Exception e = assertThrows(IllegalArgumentException.class,
									   () -> entryLogger.addEntry(ledgerId, bb));
			assertEquals(expectedEntry, e.getMessage());
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
				assertTrue(ledgerContent.contains(expectedEntry));
			} else {
				assertFalse(ledgerContent.contains(expectedEntry));
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
		} else {
			ByteBuf retrievedEntry = entryLogger.readEntry(ledgerId, entryId, realLocation);
		    assertEquals(ledgerId, retrievedEntry.readLong());
		    assertEquals(entryId, retrievedEntry.readLong());
		    byte[] data = new byte[retrievedEntry.readableBytes()];
		    retrievedEntry.readBytes(data);
		    retrievedEntry.release();
		    assertEquals(expectedEntry, new String(data));
		}
	}
	
	@After
    public void cleanUp() throws Exception {
        if (null != this.entryLogger) {
            entryLogger.close();
        }
        FileUtils.deleteDirectory(ledgerDir);
	}

//	@Test
//    public void extractEntryLogMetadataFromIndexTest() {
//        EntryLogMetadata entryLogMeta =  defEntryLog.extractEntryLogMetadataFromIndex(entryLogId);
//        assertEquals(expectedMeta, entryLogMeta.toString());
//    }

}

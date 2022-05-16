package org.apache.bookkeeper.bookie;

import static org.apache.bookkeeper.bookie.BookKeeperServerStats.BOOKIE_SCOPE;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.LD_INDEX_SCOPE;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.LD_LEDGER_SCOPE;
import static org.junit.Assert.assertTrue;
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

	@Rule public MockitoRule rule = MockitoJUnit.rule();

	@Mock
	Supplier<BookieServiceInfo> bookieServiceInfoProvider;
	@Mock
	RegistrationManager rm;
	@Mock
	LedgerManager ledgerManager;

	private DefaultEntryLogger entryLogger;
	private File ledgerDir;
	private boolean validEntry;
	// addEntryTest parameters
	private String expectedEntry;
	private long ledgerId;
	private ByteBuffer bb;

	@Parameters
	public static Collection<Object[]> data() {
		return Arrays.asList(new Object[][] {
			// expected, ledgerId, entry, valid entry
			{"Keys and values must be >= 0", -1, generateEntry(-1, 0), true},
			{"TEST[0,0]", 0, generateEntry(0, 0), true},
			{"TEST[1,0]", 1, generateEntry(1, 0), true},
			{null, 1, null, false},
			{"Invalid entry", 1, generateInvalidEntry("Invalid entry"), false},
		});
	}

	public DefaultEntryLoggerTest(String expectedEntry, long ledgerId, ByteBuffer bb, boolean validEntry) throws Exception {
		configure(expectedEntry, ledgerId, bb, validEntry);
	}

	public void configure(String expectedEntry, long ledgerId, ByteBuffer bb, boolean validEntry) throws Exception {
		this.expectedEntry = expectedEntry;
		this.ledgerId = ledgerId;
		this.bb = bb;
		this.validEntry = validEntry;
		
		ServerConfiguration conf = new ServerConfiguration();
		// build a bookie
		BookieImpl bookie = bookieBuilder(conf);
		// instance the class under test
		entryLogger = new DefaultEntryLogger(conf, bookie.getLedgerDirsManager());
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
    
    private static ByteBuffer generateInvalidEntry(String text) {
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
			if (validEntry) {
				assertTrue(ledgerContent.contains(expectedEntry));
			} else {
				assertFalse(ledgerContent.contains(expectedEntry));
			}
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
	
//	  retrievedEntry = entryLogger.readEntry(ledgerId, 0, location);
//    retrievedEntry.readLong();
//    retrievedEntry.readLong();
//    byte[] data = new byte[retrievedEntry.readableBytes()];
//    retrievedEntry.readBytes(data);
//    retrievedEntry.release();
//    assertEquals(expectedEntry, new String(data));
}

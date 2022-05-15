package org.apache.bookkeeper.bookie;

import static org.apache.bookkeeper.bookie.BookKeeperServerStats.BOOKIE_SCOPE;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.LD_INDEX_SCOPE;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.LD_LEDGER_SCOPE;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.File;
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
	// addEntryTest parameters
	private String expectedEntry;
	private long ledgerId;
	private long entryId;

	@Parameters
	public static Collection<Object[]> data() {
		return Arrays.asList(new Object[][] {
			// expected, ledgerId, entryId
			{"TEST[0,0]", 0, 0},
		});
	}

	public DefaultEntryLoggerTest(String expectedEntry, long ledgerId, long entryId) throws Exception {
		configure(expectedEntry, entryId, ledgerId);
	}

	public void configure(String expectedEntry, long ledgerId, long entryId) throws Exception {
		this.expectedEntry = expectedEntry;
		this.ledgerId = ledgerId;
		this.entryId = entryId;
		// build a bookie
		ServerConfiguration conf = new ServerConfiguration();
		BookieImpl bookie = bookieBuilder(conf);
		// instance the class under test
		entryLogger = new DefaultEntryLogger(conf, bookie.getLedgerDirsManager());

	}

	private BookieImpl bookieBuilder(ServerConfiguration conf) throws Exception {
		File ledgerDir = IOUtils.createTempDir("bkTest", ".dir");
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

    private static ByteBuf generateEntry(long ledger, long entry) {
        byte[] data = generateDataString(ledger, entry).getBytes();
        ByteBuf bb = Unpooled.buffer(8 + 8 + data.length);
        bb.writeLong(ledger);
        bb.writeLong(entry);
        bb.writeBytes(data);
        return bb;
    }
    
    private static String generateDataString(long ledgerId, long entryId) {
        return ("TEST[" + ledgerId + "," + entryId + "]");
    }

	@Test
	public void addEntryTest() throws Exception {
		// add entry
		long location = entryLogger.addEntry(ledgerId, generateEntry(ledgerId, entryId).nioBuffer());
        entryLogger.flush();
        // verify written entry
        ByteBuf retrievedEntry = entryLogger.readEntry(ledgerId, entryId, location);
        retrievedEntry.readLong();
        retrievedEntry.readLong();
        byte[] data = new byte[retrievedEntry.readableBytes()];
        retrievedEntry.readBytes(data);
        retrievedEntry.release();
        assertEquals(expectedEntry, new String(data));
	}
	
	@After
    public void cleanUp() throws Exception {
        if (null != this.entryLogger) {
            entryLogger.close();
        }
	}

//	@Test
//    public void extractEntryLogMetadataFromIndexTest() {
//        EntryLogMetadata entryLogMeta =  defEntryLog.extractEntryLogMetadataFromIndex(entryLogId);
//        assertEquals(expectedMeta, entryLogMeta.toString());
//    }
}

package org.apache.bookkeeper.bookie;

import static org.apache.bookkeeper.bookie.BookKeeperServerStats.BOOKIE_SCOPE;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.LD_INDEX_SCOPE;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.LD_LEDGER_SCOPE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import org.apache.bookkeeper.bookie.Bookie.NoLedgerException;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.server.conf.BookieConfiguration;
import org.apache.bookkeeper.server.service.StatsProviderService;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.util.DiskChecker;
import org.apache.bookkeeper.util.IOUtils;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

@RunWith (value=Parameterized.class)
public class InterleavedLedgerStorageTest {

	private InterleavedLedgerStorage storage;
	private ByteBuf entry;
	private File ledgerDir;
	private String expected;

	private long ledgerId;
	private long entryId;
	
	@Parameters
	public static Collection<Object[]> data() {
		return Arrays.asList(new Object[][] {
			{null, -1, 0},
			{"TEST[0,1]", 0, 1},
			{"TEST[1,1]", 1, -1},
		});
	}
	
	public InterleavedLedgerStorageTest(String expected, long ledgerId, long entryId) throws Exception {
		configure(expected, ledgerId, entryId);
	}
	
	public void configure(String expected, long ledgerId, long entryId) throws Exception {
		this.expected = expected;
		this.ledgerId = ledgerId;
		this.entryId = entryId;
		
		storage = new InterleavedLedgerStorage();
		// prepare for initialization
		ledgerDir = IOUtils.createTempDir("bkTest", ".dir");
		ServerConfiguration conf = new ServerConfiguration();
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

        ByteBufAllocator allocator = BookieResources.createAllocator(conf);
		// initialize storage
		storage.initialize(conf, null, ledgerDirsManager, indexDirsManager, rootStatsLogger, allocator);
		// avoid addEntry method exceptions
		ledgerId = (ledgerId < 0) ? -ledgerId : ledgerId;
		entryId = (entryId < 0) ? -entryId : entryId;
		// add entry
		entry = EntryGenerator.generateEntry(ledgerId, entryId);
		storage.setMasterKey(ledgerId, "testKey".getBytes());
		storage.addEntry(entry);
	}
	
	@Test
	public void getEntryTest() throws IOException {
		if (ledgerId < 0) {
			assertThrows(NoLedgerException.class,
						 () -> storage.getEntry(ledgerId, entryId));
		} else {
			ByteBuf retrievedEntry = storage.getEntry(ledgerId, entryId);
			assertEquals(entry.readLong(), retrievedEntry.readLong());
			assertEquals(entry.readLong(), retrievedEntry.readLong());
			byte[] data = new byte[retrievedEntry.readableBytes()];
		    retrievedEntry.readBytes(data);
		    retrievedEntry.release();
		    assertEquals(expected, new String(data));
		}
	}
	
	@After
    public void cleanUp() {
		try {
			FileUtils.deleteDirectory(ledgerDir);
		} catch(Exception e) {

		}
        
	}
}

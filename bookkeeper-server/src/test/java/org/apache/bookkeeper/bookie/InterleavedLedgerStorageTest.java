package org.apache.bookkeeper.bookie;

import static org.apache.bookkeeper.bookie.BookKeeperServerStats.BOOKIE_SCOPE;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.LD_INDEX_SCOPE;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.LD_LEDGER_SCOPE;
import static org.junit.Assume.assumeTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Optional;

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

import com.google.common.util.concurrent.RateLimiter;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

@RunWith (value=Parameterized.class)
public class InterleavedLedgerStorageTest {

	enum Type {GET_ENTRY, CONSISTENCY_CHECK};

	// used by all tests
	private InterleavedLedgerStorage storage;
	private ByteBuf entry;
	private File ledgerDir;
	private Type type;
	
	// common tests parameters
	private String expected;

	// getEntry parameters
	private long ledgerId;
	private long entryId;
	
	// localConsistencyCheck parameters
	private Optional<RateLimiter> rateLimiter;
	
	@Parameters
	public static Collection<Object[]> data() {
		return Arrays.asList(new Object[][] {
			// type, expected, ledgerId, entryId
			{Type.GET_ENTRY, null, Long.valueOf(-1), Long.valueOf(0), null},
			{Type.GET_ENTRY, "TEST[0,1]", Long.valueOf(0), Long.valueOf(1),  null},
			{Type.GET_ENTRY, "TEST[1,1]", Long.valueOf(1), Long.valueOf(-1), null},
			// type, expected, null, null, rateLimiter
			{Type.CONSISTENCY_CHECK, "[]", null, null, null},
			{Type.CONSISTENCY_CHECK, "[]", null, null, Optional.of(RateLimiter.create(1))},
		});
	}
	
	public InterleavedLedgerStorageTest(Type type, String expected, Long ledgerId, Long entryId, Optional<RateLimiter> rateLimiter) throws Exception {
		if (type == Type.GET_ENTRY)
			configure(type, expected, ledgerId, entryId);
		else
			configure(type, expected, rateLimiter);
	}
	
	public void configure(Type type, String expected, Long ledgerId, Long entryId) {
		this.type = type;
		this.expected = expected;
		this.ledgerId = ledgerId.longValue();
		this.entryId = entryId.longValue();
		
		try {
			storage = new InterleavedLedgerStorage();
			initializeStorage(storage);
			// avoid addEntry method exceptions
			ledgerId = (ledgerId < 0) ? -ledgerId : ledgerId;
			entryId = (entryId < 0) ? -entryId : entryId;
			// add entry
			entry = EntryGenerator.create(ledgerId, entryId);
			storage.setMasterKey(ledgerId, "testKey".getBytes());
			storage.addEntry(entry);
			storage.flush();
		} catch(Exception e) {
			// do nothing
		}
	}
	
	public void configure(Type type, String expected, Optional<RateLimiter> rateLimiter) throws Exception {
		this.type = type;
		this.expected = expected;
		this.rateLimiter = rateLimiter;
		
		storage = new InterleavedLedgerStorage();
		initializeStorage(storage);
	}
	
	private void initializeStorage(InterleavedLedgerStorage storage) throws Exception {
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
	}
	
	@Test
	public void getEntryTest() throws IOException {
		assumeTrue(type == Type.GET_ENTRY);
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
	
	@Test
	public void localConsistencyCheckTest() throws IOException {
		assumeTrue(type == Type.CONSISTENCY_CHECK);
		assertEquals(expected, storage.localConsistencyCheck(rateLimiter).toString());
	}
	
	@After
    public void cleanUp() {
		try {
			FileUtils.deleteDirectory(ledgerDir);
		} catch(Exception e) {
			// do nothing
		}
        
	}
}

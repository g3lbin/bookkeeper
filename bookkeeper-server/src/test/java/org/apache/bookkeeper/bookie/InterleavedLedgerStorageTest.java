package org.apache.bookkeeper.bookie;

import static org.apache.bookkeeper.bookie.BookKeeperServerStats.BOOKIE_SCOPE;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.LD_INDEX_SCOPE;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.LD_LEDGER_SCOPE;
import static org.junit.Assume.assumeTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
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
	private long entryToBeGot;
	private long entryToAdd;
	private boolean corruptEntry;
	
	// localConsistencyCheck parameters
	private Optional<RateLimiter> rateLimiter;
	
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
			// type, expected, ledgerId, entryToBeGot, null, entryToAdd, corruptEntry
			{Type.GET_ENTRY, null, L(-1), L(0), null, L(0), B(false)},
			{Type.GET_ENTRY, "TEST[0,1]", L(0), L(1),  null, L(1), B(false)},
			{Type.GET_ENTRY, "TEST[1,1]", L(1), L(-1), null, L(-1), B(false)},
			{Type.GET_ENTRY, null, L(0), L(1),  null, L(0), B(false)},
			{Type.GET_ENTRY, null, L(0), L(0),  null, L(0), B(true)},
			// type, expected, null, null, rateLimiter, null, null
			{Type.CONSISTENCY_CHECK, "[]", null, null, null, null, null},
			{Type.CONSISTENCY_CHECK, "[]", null, null, Optional.of(RateLimiter.create(1)), null, null},
		});
	}
	
	public InterleavedLedgerStorageTest(Type type, String expected, Long longParam1, Long longParam2,
			Optional<RateLimiter> rateLimiter, Long longParam3, Boolean boolParam1) throws Exception {
		if (type == Type.GET_ENTRY)
			configure(type, expected, longParam1, longParam2, longParam3, boolParam1);
		else
			configure(type, expected, rateLimiter);
	}
	
	public void configure(Type type, String expected, Long ledgerId, Long entryToBeGot, Long entryToAdd, Boolean corruptEntry) {
		this.type = type;
		this.expected = expected;
		this.ledgerId = ledgerId.longValue();
		this.entryToBeGot = entryToBeGot.longValue();
		this.corruptEntry = corruptEntry.booleanValue();
		
		try {
			storage = new InterleavedLedgerStorage();
			initializeStorage(storage);
			// avoid addEntry method exceptions
			ledgerId = (ledgerId < 0) ? -ledgerId : ledgerId;
			this.entryToAdd = (entryToAdd < 0) ? -entryToAdd : entryToAdd;
			// add entry
			entry = EntryGenerator.create(ledgerId, entryToAdd);
			storage.setMasterKey(ledgerId, "testKey".getBytes());
			storage.addEntry(entry);
			storage.flush();
			
			if (this.corruptEntry)
				incEntryId();
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
	
	private void incEntryId() throws IOException {
		File fileLog = new File(ledgerDir.getAbsolutePath() + "/current/0.log");
        RandomAccessFile raf = new RandomAccessFile(fileLog, "rw");
        try {
            raf.seek(DefaultEntryLogger.LOGFILE_HEADER_SIZE + 8);
            long entryId = raf.readLong();
            raf.seek(DefaultEntryLogger.LOGFILE_HEADER_SIZE + 8);
            raf.writeLong(entryId + 1);
        } finally {
            raf.close();
        }
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
		if (corruptEntry) {
			assertThrows(IOException.class,
						 () -> storage.getEntry(ledgerId, entryToBeGot));
		} else if (entryToAdd != entryToBeGot) {
			assertThrows(Bookie.NoEntryException.class,
						 () -> storage.getEntry(ledgerId, entryToBeGot));
		} else if (ledgerId < 0) {
			assertThrows(NoLedgerException.class,
						 () -> storage.getEntry(ledgerId, entryToBeGot));
		} else {
			ByteBuf retrievedEntry = storage.getEntry(ledgerId, entryToBeGot);
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

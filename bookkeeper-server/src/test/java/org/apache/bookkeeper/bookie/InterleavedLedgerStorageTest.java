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
import java.util.List;
import java.util.Optional;

import org.apache.bookkeeper.bookie.Bookie.NoLedgerException;
import org.apache.bookkeeper.bookie.LedgerCache.PageEntriesIterable;
import org.apache.bookkeeper.bookie.LedgerStorage.DetectedInconsistency;
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
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

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
	private long ledgerId;
	private boolean corruptEntry;

	// getEntry parameters
	private long entryToBeGot;
	private long entryToAdd;
	
	// localConsistencyCheck parameters
	private long entryId;
	private Optional<RateLimiter> rateLimiter;
	private boolean corruptCheck;
	
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

			// type, expected, ledgerId, entryToBeGot, null, entryToAdd, corruptEntry, null
			{Type.GET_ENTRY, null, L(-1), L(0), null, L(0), B(false), null},
			{Type.GET_ENTRY, "TEST[0,1]", L(0), L(1),  null, L(1), B(false), null},
			{Type.GET_ENTRY, "TEST[1,1]", L(1), L(-1), null, L(-1), B(false), null},
			{Type.GET_ENTRY, null, L(0), L(1),  null, L(0), B(false), null},
			{Type.GET_ENTRY, null, L(0), L(0),  null, L(0), B(true), null},
			// type, expected, ledgerId, entryId, rateLimiter, null, corruptEntry, corruptCheck
			{Type.CONSISTENCY_CHECK, null, L(0), L(0), null, null, B(false), B(false)},
			{Type.CONSISTENCY_CHECK, "[]", L(1), L(0), Optional.of(RateLimiter.create(1)), null, B(false), B(false)},
			{Type.CONSISTENCY_CHECK, null, L(1), L(2), Optional.of(RateLimiter.create(0.1)), null, B(true), B(false)},
			{Type.CONSISTENCY_CHECK, "[]", null, null, Optional.of(RateLimiter.create(0.0001)), null, B(false), B(false)},
			{Type.CONSISTENCY_CHECK, "[]", L(0), L(2), Optional.of(RateLimiter.create(10)), null, B(false), B(true)},
		});
	}
	
	public InterleavedLedgerStorageTest(Type type, String expected, Long longParam1, Long longParam2,
			Optional<RateLimiter> rateLimiter, Long longParam3, Boolean boolParam1, Boolean boolParam2) throws Exception {
		if (type == Type.GET_ENTRY)
			configure(type, expected, longParam1, longParam2, longParam3, boolParam1);
		else
			configure(type, expected, longParam1, longParam2, rateLimiter, boolParam1, boolParam2);
	}
	
	public void configure(Type type, String expected, Long ledgerId, Long entryToBeGot, 
			Long entryToAdd, Boolean corruptEntry) {
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
		} catch (Exception e) {
			// do nothing
		}
	}
	
	public void configure(Type type, String expected, Long ledgerId, Long entryId,
			Optional<RateLimiter> rateLimiter,  Boolean corruptEntry, Boolean corruptCheck) throws Exception {
		this.type = type;
		this.expected = expected;
		this.rateLimiter = rateLimiter;
		this.corruptEntry = corruptEntry.booleanValue();
		this.corruptCheck = corruptCheck.booleanValue();
		
		storage = new InterleavedLedgerStorage();
		initializeStorage(storage);
		
		if (ledgerId != null && entryId != null) {
			// avoid addEntry method exceptions
			this.ledgerId = (ledgerId < 0) ? -ledgerId : ledgerId;
			this.entryId = (entryId < 0) ? -entryId : entryId;
		} else {
			this.ledgerId = 0L;
			this.entryId = 0L;
		}
		// add entry
		entry = EntryGenerator.create(this.ledgerId, this.entryId);
		storage.setMasterKey(this.ledgerId, "testKey".getBytes());
		storage.addEntry(entry);
		storage.flush();
	
		if (this.corruptEntry)
	        incEntryId();
		
		if (ledgerId == null || entryId == null) {
			LedgerCache ledgerCache = Mockito.spy(storage.ledgerCache);
			PageEntriesIterable pages = storage.ledgerCache.listEntries(this.ledgerId);
			Mockito.doAnswer(new Answer<PageEntriesIterable>() {
				@Override
				public PageEntriesIterable answer(InvocationOnMock invocation) throws Throwable {
					storage.deleteLedger(0L);
					return pages;
				}
			}).when(ledgerCache).listEntries(Mockito.anyLong());
			storage.ledgerCache = ledgerCache;
			this.entryId = -1L;
		} else if (corruptCheck) {
			DefaultEntryLogger entryLogger = Mockito.spy(storage.entryLogger);
			Mockito.doAnswer(new Answer<Void>() {
				@Override
				public Void answer(InvocationOnMock invocation) throws Throwable {
					storage.deleteLedger(ledgerId.longValue());
					throw new DefaultEntryLogger.EntryLookupException("Mocked checkEntry");
				}
			}).when(entryLogger).checkEntry(Mockito.anyLong(), Mockito.anyLong(), Mockito.anyLong());
			storage.entryLogger = entryLogger;
		}
		
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
		if (entryId == -1L || corruptCheck) {
			assertEquals(expected, storage.localConsistencyCheck(rateLimiter).toString());
		} else if (corruptEntry) {
			List<DetectedInconsistency> list = storage.localConsistencyCheck(rateLimiter);
			assertEquals(1, list.size());
			DetectedInconsistency error = list.get(0);
			assertEquals(ledgerId, error.getLedgerId());
			assertEquals(entryId, error.getEntryId());
		} else if (rateLimiter != null) {
			assertEquals(expected, storage.localConsistencyCheck(rateLimiter).toString());
		} else {
			assertThrows(IOException.class,
					 	 () -> storage.localConsistencyCheck(rateLimiter));
		}
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

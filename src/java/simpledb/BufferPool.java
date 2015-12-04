package simpledb;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 * 
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /** Bytes per page, including header. */
    private static final int PAGE_SIZE = 4096;

    private static int pageSize = PAGE_SIZE;
    
    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    private ConcurrentHashMap<PageId, Page> pool;
    private Locker locker;
    private int numPages;
    private PageId mostRecent = null;
    
    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        this.numPages = numPages;
        this.pool = new ConcurrentHashMap<PageId, Page>();
        this.locker = new Locker();
    }
    
    public static int getPageSize() {
      return pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
    	BufferPool.pageSize = pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
    	BufferPool.pageSize = PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, an page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
        if (this.pool.size() == this.numPages) {
        	this.evictPage();
        }

        if (!(this.pool.containsKey(pid))) {
        	Page p = Database
        				 .getCatalog()
        				 .getDatabaseFile(pid.getTableId())
        				 .readPage(pid);
        	p.setBeforeImage();
	        this.pool.put(pid, p);
        }

        this.mostRecent = pid;
        this.locker.acquireLock(tid, pid, perm);
        return this.pool.get(pid);
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public void releasePage(TransactionId tid, PageId pid) {
        this.locker.releaseLock(tid, pid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        this.transactionComplete(tid, true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        return this.locker.holdsLock(tid, p);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit)
        throws IOException {
    	// Check that the transaction has not already been committed.
    	if (this.locker.getPages(tid) == null) {
    		return;
    	}

        if (commit) {
            Iterator<PageId> it = this.locker.getPages(tid);
            while (it.hasNext()) {
            	PageId pid = it.next();
            	Page p = this.pool.get(pid);
            	if (p != null) {
                	// Potentially add a check to ensure that all flushed pages are
                	// correctly marked for the given transaction.
                	this.flushPage(pid);
                	
                	// Use current page contents as the before-image
                	// for the next transaction that modifies this page.
                	this.pool.get(pid).setBeforeImage();
            	}
            }
        } else {
        	Iterator<PageId> it = this.locker.getPages(tid);
        	while (it.hasNext()) {
        		PageId pid = it.next();
        		if (this.pool.containsKey(pid)) {
	        		Page p = this.pool.get(pid);
	
	        		// It should be enough to check that isPageDirty returns a
	        		// non-null value, but this ensures that it was dirtied by
	        		// the correct transaction.
	        		if (p.isPageDirty() != null &&
	        			p.isPageDirty().equals(tid)) {
	        			this.pool.put(pid, p.getBeforeImage());
	        		}
        		}
        	}
        }

        this.locker.releaseAllLocks(tid);
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other 
     * pages that are updated (Lock acquisition is not needed for lab2). 
     * May block if the lock(s) cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markPageDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
    	ArrayList<Page> changed =
    		Database.getCatalog().getDatabaseFile(tableId).insertTuple(tid, t);
    	
    	Iterator<Page> it = changed.iterator();
    	while (it.hasNext()) {
    		Page p = it.next();
    		p.markPageDirty(true, tid);
    		this.pool.put(p.getId(), p);
    	}
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markPageDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        ArrayList<Page> changed =
        	Database
	            .getCatalog()
	            .getDatabaseFile(t.getRecordId().getPageId().getTableId())
	            .deleteTuple(tid, t);
        
    	Iterator<Page> it = changed.iterator();
    	while (it.hasNext()) {
    		Page p = it.next();
    		p.markPageDirty(true, tid);
    		this.pool.put(p.getId(), p);
    	}
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        Iterator<PageId> pageIds = this.pool.keySet().iterator();
        while (pageIds.hasNext()) {
        	this.flushPage(pageIds.next());
        }
    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
        
        Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
    */
    public synchronized void discardPage(PageId pid) {
        this.pool.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized void flushPage(PageId pid) throws IOException {
    	Page p = this.pool.get(pid);
    	if (p != null) {
	    	TransactionId tid = p.isPageDirty();
	    	if (tid != null) {
	    	    // Append an update record to the log, with 
	    	    // a before-image and after-image.
	    		Database.getLogFile().logWrite(tid, p.getBeforeImage(), p);
	    	    Database.getLogFile().force();
	    	}

    		Database
    			.getCatalog()
    			.getDatabaseFile(pid.getTableId())
    			.writePage(this.pool.get(pid));

        	p.markPageDirty(false, tid);
    	}
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized void flushPages(TransactionId tid) throws IOException {
        Iterator<PageId> it = this.locker.getPages(tid);
        while (it.hasNext()) {
        	// Potentially add a check to ensure that all flushed pages are
        	// correctly marked for the given transaction.
        	this.flushPage(it.next());
        }
    }

    /**
     * Discards a page from the buffer pool.
     * Only evicts dirty pages as part of the NO STEAL/FORCE policy.
     */
    private synchronized void evictPage() throws DbException {
    	PageId evict = this.mostRecent;
    	Iterator<PageId> it = this.pool.keySet().iterator();
		while (it.hasNext() && (this.pool.get(evict).isPageDirty() != null)) {
			evict = it.next();
		}
		
		if (this.pool.get(evict).isPageDirty() != null) {
			throw new DbException(
						  "Cannot evict a page because all pages are dirty.");
		}

        this.discardPage(evict);
    }

}

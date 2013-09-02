package simpledb;


import java.io.*;
import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;


/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 */
public class BufferPool {
    /** Bytes per page, including header. */
    public static final int PAGE_SIZE = 4096;


    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;


    private int numPages;
    private HashMap<Integer, Page> cachedPages;
    private LinkedList<Integer> recentlyUsed;
    private TransactionLockManager lockManager;


    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        this.numPages = numPages;
        this.cachedPages = new HashMap<Integer, Page>();
        this.recentlyUsed = new LinkedList<Integer>();
        this.lockManager = new TransactionLockManager();
    }


    private boolean isFull() {
    	assert (recentlyUsed.size() == cachedPages.size());
    	return cachedPages.size() >= numPages;
    }
    
    
    // Forcibly evict this page from the buffer pool.
    // Bypasses consistency checks and does not flush to disk
    // Used by the Log file to clean up the buffer pool
    public void evictPage(Page page) {
    	PageId pid = page.getId();
    	int hashcode = pid.hashCode();
    	
    	if (inCache(pid)) {
	    	assert (this.recentlyUsed.contains(hashcode));
	    	this.cachedPages.remove(hashcode);
	    	int lruIndex = this.recentlyUsed.indexOf(hashcode);
	    	this.recentlyUsed.remove(lruIndex);
    	} else {
	    	assert (!this.recentlyUsed.contains(hashcode));
    	}
    }
    
    public void clean() {
    	assert (this.cachedPages.isEmpty());
    	assert (this.recentlyUsed.isEmpty());
    }


	public static int getPageSize() {
		// TODO Auto-generated method stub
		return PAGE_SIZE;
	}
    
    private void refreshUse(PageId pid) {
    	int hashCode = pid.hashCode();
    	assert (recentlyUsed.contains(hashCode));
    	int index = recentlyUsed.indexOf(hashCode);
    	recentlyUsed.remove(index);
    	recentlyUsed.addFirst(hashCode);
    }
    
    synchronized void checkConsistency() {
    	//System.out.println("Recently used: " + recentlyUsed.size());
    	//System.out.println("Cached pages: " + cachedPages.size());
    	//assert (recentlyUsed.size() == cachedPages.size());
    }
    
   
    
    private boolean inCache(PageId pid) {
    	int hashcode = pid.hashCode();
    	return cachedPages.containsKey(hashcode);
    }
    
    private synchronized void recoverPage(TransactionId tid, PageId pid) {
    	if (isRecoverable(pid)) {
    		int hashCode = pid.hashCode();
    		Page page = this.cachedPages.get(hashCode);
    		this.cachedPages.put(hashCode,  page.getBeforeImage());
    		refreshUse(pid);
    		boolean isDirty = false;
    		page.markDirty(isDirty, null);
    		return;
    	}
    	// Otherwise it was a read lock and we don't have to do anything
    	// to recover. Our eviction strategy guarantees we don't evict
    	// dirty pages so we should only recover dirty pages in the buffer pool
    	//System.out.println("Asked to recover page: " + pid.pageNumber());
    	assert (this.lockManager.hasLock(tid, pid, Permissions.READ_ONLY));
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
    public synchronized Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
    	int pageHash = pid.hashCode();    	
        if (!inCache(pid)) {
        	// Holding a lock shouldn't matter! Buffer pool is independent of lock manager
        	if (isFull()) {
            	evictPage();
            	assert (!isFull());
            }


            int tableId = pid.getTableId();
            DbFile file = Database.getCatalog().getDatabaseFile(tableId);
            
            cachedPages.put(pageHash, file.readPage(pid));
            this.recentlyUsed.addFirst(pageHash);
        }


        refreshUse(pid);
        checkConsistency();
        Page page =this.cachedPages.get(pageHash);


        this.lockManager.getLock(tid,  pid,  perm);
        return page;
    }
    
    
    synchronized void checkConsistency(PageId pid) {
    	int hashCode = pid.hashCode();
    	assert (recentlyUsed.contains(hashCode));
    	assert (cachedPages.containsKey(hashCode));
    	checkConsistency();
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
    public synchronized void releasePage(TransactionId tid, PageId pid) {
    	this.lockManager.clearLock(tid, pid);
    }


    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public synchronized void transactionComplete(TransactionId tid) throws IOException {
    	boolean commit = true;
    	transactionComplete(tid, commit);
    }


    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId pid, Permissions permission) {
    	return this.lockManager.hasLock(tid, pid, permission);
    }
    
    /***
     * Returns true if the specific page id is 
     * a) in the buffer cache
     * b) Has a write lock
     * c) Is actually dirty and needs to be flushed
     * If we only have a read lock, then no need to flush
     * If it isn't in the buffer cache then no need to flush as well
     * because eviction strategy only evicts non-dirty pages
     * and to get a dirty page, we need a write lock
     * @param pid
     * @return
     */
    private boolean isRecoverable(PageId pid) {
    	int hashcode = pid.hashCode();
    	if (this.cachedPages.containsKey(hashcode) && 
    			this.lockManager.hasWriteLock(pid)) {
    		Page page = this.cachedPages.get(hashcode);
    		TransactionId tid = page.isDirty();
    		return tid != null;
    	}
    	
    	return false;
    }
    
    
    
    private synchronized void recoverPages(TransactionId tid) {
    	for (PageId pid : this.lockManager.getPagesInTransaction(tid)) {
    		recoverPage(tid, pid);
    	}
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
    	if (commit) {
    		commitTransaction(tid);
    	} else {
    		recoverPages(tid);
    	}
    	
    	this.lockManager.clearAllLocks(tid);
    }


	private void commitTransaction(TransactionId tid) throws IOException {
		for (PageId pid : this.lockManager.getPagesInTransaction(tid)) {
			int hashcode = pid.hashCode();
			if (isRecoverable(pid)) {
				flushPage(pid, tid);
			} 


			// If the page wasn't in cache, it meant we only had a read lock
			// on the page. We don't have to set before image unless we had a 
			// write lock. Another transaction could not have a write lock
			// Otherwise this transaction couldn't finish.
			if (inCache(pid)) {
				Page page = this.cachedPages.get(hashcode);
				page.setBeforeImage();
			}
		}
	}


    /**
     * Add a tuple to the specified table behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to(Lock 
     * acquisition is not needed for lab2). May block if the lock cannot 
     * be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and updates cached versions of any pages that have 
     * been dirtied so that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
    	DbFile file = Database.getCatalog().getDatabaseFile(tableId);
    	file.insertTuple(tid, t);
    }


    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from. May block if
     * the lock cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit.  Does not need to update cached versions of any pages that have 
     * been dirtied, as it is not possible that a new page was created during the deletion
     * (note difference from addTuple).
     *
     * @param tid the transaction adding the tuple.
     * @param t the tuple to add
     */
    public void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, TransactionAbortedException {
    	PageId pid = t.getRecordId().getPageId();
    	DbFile file = Database.getCatalog().getDatabaseFile(pid.getTableId());
    	file.deleteTuple(tid, t);
    }


    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
    	for (int pageHash : this.cachedPages.keySet()) {
    		Page page = this.cachedPages.get(pageHash);
    		flushPage(page.getId(), page.isDirty());
    	}
    }


    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
    */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // only necessary for lab5
    	assert (false);
    	// Equivalent to our evictPage at the bottom
    }


    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private void flushPage(PageId pid, TransactionId tid) throws IOException {
    	DbFile file = Database.getCatalog().getDatabaseFile(pid.getTableId());
    	int pageHash = pid.hashCode();
    	assert (inCache(pid));
    	Page page = this.cachedPages.get(pageHash);
    	
    	if (tid != null) {
    		writeLog(tid, page);
    		writePage(tid, file, page);
    	}
    }


	private void writePage(TransactionId tid, DbFile file, Page page)
			throws IOException {
		file.writePage(page);
		boolean isDirty = false;
		page.markDirty(isDirty, tid);
	}


	private void writeLog(TransactionId tid, Page page) throws IOException {
		LogFile log = Database.getLogFile();
		log.logWrite(tid, page.getBeforeImage(), page);
		log.force();
	}


    /** Write all pages of the specified transaction to disk.
     */
    public void flushPages(TransactionId tid) throws IOException {
    	for (PageId pid : this.lockManager.getPagesInTransaction(tid)) {
    		if (isRecoverable(pid)) {
    			flushPage(pid, tid);
    		} 
    	}
    }


    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized void evictPage() throws DbException {
    	assert (this.cachedPages.size() == this.recentlyUsed.size());
    	assert (this.recentlyUsed.size() <= this.numPages);
    	
    	//System.out.println("Evicting page unmber of pages: " + this.numPthis.ges);
    	try {
    		for (int i = 0; i < this.numPages; i++) {
    			int lastUsed  =this.recentlyUsed.removeLast();
    			assert (this.cachedPages.containsKey(lastUsed));
    			Page page = this.cachedPages.get(lastUsed);
    			assert (page.getId().hashCode() == lastUsed);
    			
    			if (page.isDirty() == null) {
    				flushPage(page.getId(), null);
        			this.cachedPages.remove(lastUsed);
        			//System.out.println("Evicting page: " + page.getId().pageNumber());
        			return;
    			} else {
    				this.recentlyUsed.addFirst(lastUsed);
    				assert (this.cachedPages.size() == this.recentlyUsed.size());
    			}
    		}
    		
    		throw new DbException("Cannot evict a page. All in transaction");
    	} catch (IOException e) {
    		System.out.println("Error evicting page");
    		System.out.println(e.getMessage());
    		System.exit(1);
    	}
    }
    
   


} 

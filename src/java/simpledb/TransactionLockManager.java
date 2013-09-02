package simpledb;


import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;


public class TransactionLockManager {
	private TransactionGraph transactionInfo;
	private HashMap<Integer, Semaphore> pageLocks;
	private static final int MAX_AVAILABLE = 50;


	public TransactionLockManager() {
		this.transactionInfo = new TransactionGraph();
		this.pageLocks = new HashMap<Integer, Semaphore>();
	}


	// Returns true if the specific transaction has a write lock 
	// on the given page
	public boolean hasWriteLock(TransactionId tid, PageId pid) {
		Semaphore pageLock = getPageLock(pid);
		if (pageLock.availablePermits() > 0) return false;
		return this.transactionInfo.writesPage(tid, pid);
	}


	// returns true if the specific page is locked by any transaction
	public boolean hasWriteLock(PageId pid) {
		return this.transactionInfo.hasWriteLock(pid);
	}


	private Semaphore getPageLock(PageId pid) {
		int hashCode = pid.hashCode();
		if (!this.pageLocks.containsKey(hashCode)) {
			this.pageLocks.put(hashCode, new Semaphore(MAX_AVAILABLE));
		}


		return this.pageLocks.get(hashCode);
	}


	public HashSet<PageId> getPagesInTransaction(TransactionId tid) {
		return this.transactionInfo.pagesInTransaction(tid);
	}


	private void addTransactionPageLinks(TransactionId tid, PageId pid, Permissions perm) {
		this.transactionInfo.addEdge(tid,  pid);
		if (perm == Permissions.READ_WRITE) {
			assert (this.transactionInfo.writesPage(tid, pid));
		} else {
			assert (this.transactionInfo.readsPage(tid, pid));
		}
	}


	private int locksAcquired(Semaphore lock) {
		return MAX_AVAILABLE - lock.availablePermits();
	}


	public boolean hasLock(TransactionId tid, PageId pid, Permissions permission) {
		if (permission == Permissions.READ_WRITE) {
			return hasWriteLock(tid, pid);
		}


		return this.transactionInfo.readsPage(tid, pid);
	}


	// returns true if ONLY one transaction has a read lock on the page
	private boolean isUpgradeable(TransactionId tid, PageId pid) {
		if (!hasLock(tid, pid, Permissions.READ_ONLY)) return false;


		Semaphore lock = getPageLock(pid);
		if (locksAcquired(lock) != 1) return false;


		assert (this.transactionInfo.numTransactions(pid) == 1);
		return true;
	}


	private void getReadLock(TransactionId tid, PageId pid, Semaphore lock)
			throws TransactionAbortedException {
		if (!hasLock(tid, pid, Permissions.READ_ONLY)) {
			try {
				//System.out.println("Number of locks: " + lock.availablePermits());
				boolean acquired = lock.tryAcquire(1, 100,
						TimeUnit.MILLISECONDS);
				if (!acquired) {
					throw new TransactionAbortedException();
				}
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}


	private synchronized void getWriteLock(TransactionId tid, PageId pid,
			Semaphore lock) throws TransactionAbortedException {
		if (isUpgradeable(tid, pid)) {
			lock.release();
			assert (lock.availablePermits() == MAX_AVAILABLE);
		}


		try {
			if (!hasLock(tid, pid, Permissions.READ_WRITE)) {
				boolean aquired = lock.tryAcquire(MAX_AVAILABLE, 100,
						TimeUnit.MILLISECONDS);
				if (!aquired) {
					throw new TransactionAbortedException();
				}
			}
		} catch (InterruptedException e) {
			System.out.println("IOException getting lock");
			e.printStackTrace();
		}
	}


	public synchronized void getLock(TransactionId tid, PageId pid,
			Permissions perm) throws TransactionAbortedException {
		Semaphore lock = getPageLock(pid);


		if (perm == Permissions.READ_ONLY) {
			getReadLock(tid, pid, lock);
		} else {
			getWriteLock(tid, pid, lock);
		}


		addTransactionPageLinks(tid, pid, perm);
	}


	public synchronized void clearAllLocks(TransactionId tid) {
		// Have to make a copy of the transaction -> page list
		// Because we modify it in clearLock
		HashSet<PageId> pagesInTransaction = getPagesInTransaction(tid);
		for (PageId pid : pagesInTransaction) {
			clearLock(tid, pid);
		}
	}


	public synchronized void clearLock(TransactionId tid, PageId pid) {
		Semaphore lock = getPageLock(pid);
		if (hasWriteLock(tid, pid)) {
			lock.release(MAX_AVAILABLE);
		} else {
			lock.release();
		}


		clearTransactionPageLinks(tid, pid);
	}


	private void clearTransactionPageLinks(TransactionId tid, PageId pid) {
		this.transactionInfo.removeEdge(tid, pid);
	}
}

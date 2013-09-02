package simpledb;


import java.io.IOException;
import java.util.*;


public class HeapFileIterator implements DbFileIterator {
    private TransactionId transactionId;
    private HeapFile file;
    private int currentPageId;
    private Page currentPage;
    private int numPages;
    private Iterator<Tuple> tupleIterator;


    public HeapFileIterator(TransactionId tid, HeapFile file) {
        transactionId = tid;
        this.file = file;
        currentPageId = 0;
        numPages = file.numPages();
    }


    public void open()
        throws DbException, TransactionAbortedException {
        currentPage = readPage(currentPageId++);
        tupleIterator = currentPage.iterator();
    }


    public boolean hasNext()
        throws DbException, TransactionAbortedException {
        if (tupleIterator == null) return false;
        if (tupleIterator.hasNext()) return true;


        // If we have more pages
        while (currentPageId <= (numPages - 1)) {
        	currentPage = readPage(currentPageId++);
        	tupleIterator = currentPage.iterator();
        	if (tupleIterator.hasNext()) {
        		return true;
        	}
        } 
        
        return false;
    }


    public Tuple next()
        throws DbException, TransactionAbortedException {
        if (tupleIterator == null) {
            throw new NoSuchElementException("Tuple iterator not opened");
        }
        
        assert (tupleIterator.hasNext());
        return tupleIterator.next();
    }


    public void rewind()
        throws DbException, TransactionAbortedException {
        this.close();
        this.open();
    }


    public void close() {
        currentPageId = 0;
        tupleIterator = null;
    }


    private Page readPage(int pageNumber) 
    	throws DbException, TransactionAbortedException {
        // File == table because we do one file per table
        int tableId = file.getId();
        int pageId = pageNumber;
        HeapPageId pid = new HeapPageId(tableId, pageId);
        return Database.getBufferPool().getPage(transactionId, pid, Permissions.READ_ONLY);
		
    }
}

package simpledb;


import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.ByteChannel;
import java.nio.channels.FileChannel;
import java.util.*;


/**
 * HeapFile is an implementation of a DbFile that stores a collection
 * of tuples in no particular order.  Tuples are stored on pages, each of
 * which is a fixed size, and the file is simply a collection of those
 * pages. HeapFile works closely with HeapPage.  The format of HeapPages
 * is described in the HeapPage constructor.
 *
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {
    private File file;
    private TupleDesc tupleDesc;
    private FileChannel fileChannel;
    private HashMap<Integer, Boolean> freePage;
    
    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f the file that stores the on-disk backing store for this heap file.
     */
    public HeapFile(File f, TupleDesc td) {
        file = f;
        tupleDesc = td;
                        
        try {
        	RandomAccessFile raf = new RandomAccessFile(f, "rw");
        	fileChannel = raf.getChannel();
        	freePage = new HashMap<Integer, Boolean>();
        	markNonFreePages();
        	
        } catch (IOException e) {
        	System.err.println("error reading channel");
        	System.exit(1);
        }
    }
    
    /***
     * If we create a new heap file, we assume
     * that every page in the file descriptor is already full 
     */
    private void markNonFreePages() {
    	for (int i = 0; i < numPages(); i++) {
    		boolean isFree = false;
    		markFree(i, isFree);
    	}
    }
    
    private synchronized void markFree(int pageNumber, boolean isFree) {
    	freePage.put(pageNumber, isFree);
    }
    
    private synchronized boolean isFree(int pageNumber) {
    	return freePage.get(pageNumber);
    }


    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return file;
    }


    /**
    * Returns an ID uniquely identifying this HeapFile. Implementation note:
    * you will need to generate this tableid somewhere ensure that each
    * HeapFile has a "unique id," and that you always return the same value
    * for a particular HeapFile. We suggest hashing the absolute file name of
    * the file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
    *
    * @return an ID uniquely identifying this HeapFile.
    */
    public int getId() {
        return file.getAbsoluteFile().hashCode();
    }
    
    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return tupleDesc;
    }


    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        int pageNumber = pid.pageNumber();
        int offset = BufferPool.PAGE_SIZE * pageNumber;
        //System.out.println("Reading page: " + pageNumber);
                
        try {
        	ByteBuffer buffer = ByteBuffer.allocate(BufferPool.PAGE_SIZE);
        	assert (offset + BufferPool.PAGE_SIZE <= fileChannel.size());
            fileChannel.read(buffer, offset);
            
            HeapPageId newPage = (HeapPageId) pid;
            return new HeapPage(newPage, buffer.array());
        } catch (IOException e) {
            System.err.println("Could not read page");
            e.printStackTrace();
            assert (false);
            return null;
        }
    }


    // see DbFile.java for javadocs
    public void writePage(Page page) {
    	// We can indirectly write a new page here without allocating one
    	int pageNumber = page.getId().pageNumber();
    	int offset = pageNumber * BufferPool.PAGE_SIZE;
    	    	
    	boolean isFree = ((HeapPage) page).hasFreeSlots();
    	markFree(pageNumber, isFree);
    	
    	try {
    		ByteBuffer buffer = ByteBuffer.wrap(page.getPageData());
    		fileChannel.write(buffer, offset);
    	} catch (IOException e) {
    		System.out.println("error writing page: " + e);
    		System.exit(1);
    	}
    }


    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
    	try {
    		int pageCount = (int) Math.ceil(this.fileChannel.size() / BufferPool.PAGE_SIZE);
    		return pageCount;
    	} catch (IOException e) {
    		System.err.println("Could not get final chanel size");
    		System.exit(1);
    		return -1;
    	}
    }
    
    private synchronized HeapPage allocateNewPage() {
    	int tableId = this.getId();
    	HeapPageId pid = new HeapPageId(tableId, this.numPages());
    	byte[] data = HeapPage.createEmptyPageData();
    	try {
    		HeapPage newPage = new HeapPage(pid, data);
    		writePage(newPage);
    		return newPage;
    	} catch (IOException e) {
    		System.out.println("Error Allocating new page");
    		System.exit(1);
    	}
    	return null;
    }
    
    private synchronized HeapPage getNextFreePage(TransactionId tid)
    	throws DbException, TransactionAbortedException {
    	BufferPool pool = Database.getBufferPool();
    	//System.out.println("Number of pages: " + numPages());
    	assert (numPages() == freePage.size());
    	
    	for (int i = 0; i < this.numPages(); i++) {
    		if (isFree(i)) {
    			HeapPageId pid = new HeapPageId(this.getId(), i);
    			return (HeapPage) pool.getPage(tid,  pid, Permissions.READ_ONLY);
    		} 
    	}
    	
    	HeapPage newPage = allocateNewPage();
    	int pageNumber = newPage.getId().pageNumber();
    	boolean isFree = true;
    	markFree(pageNumber, isFree);
    	//System.out.println("Allocating new page: " + pageNumber);
    	return newPage;
    }


    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
    	BufferPool pool = Database.getBufferPool();
    	//System.out.println("Inserting tuple " + t + " in transaction: " + tid);
    	PageId pageId = getNextFreePage(tid).getId();
    	HeapPage freePage = (HeapPage) pool.getPage(tid, pageId, Permissions.READ_WRITE);
    	assert (freePage.hasFreeSlots());
    	freePage.insertTuple(t);
    	//System.out.println("Inserted tuple onto page: " + freePage.getId().pageNumber());
    	freePage.markDirty(true,  tid);
    	markFree(pageId.pageNumber(), freePage.hasFreeSlots());
    	
    	ArrayList<Page> modifiedPages = new ArrayList<Page>();
    	modifiedPages.add(freePage);
    	//System.out.println("Finished transaction: " + tid + " insert tuple: " + t);
    	return modifiedPages;
    }


    // see DbFile.java for javadocs
    public HeapPage deleteTuple(TransactionId tid, Tuple t)
        throws DbException, TransactionAbortedException {
    	BufferPool pool = Database.getBufferPool();
    	RecordId rid = t.getRecordId();
    	HeapPage page=null;
		page = (HeapPage) pool.getPage(tid,  rid.getPageId(), Permissions.READ_WRITE);
    
    	markFree(page.getId().pageNumber(), page.hasFreeSlots());
    	    	
    	page.deleteTuple(t);
    	page.markDirty(true,  tid);
    	return page;
    }


    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        return new HeapFileIterator(tid, this);
    }
}

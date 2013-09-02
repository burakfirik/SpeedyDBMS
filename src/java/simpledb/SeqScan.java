package simpledb;
import java.util.*;


/**
 * SeqScan is an implementation of a sequential scan access method that reads
 * each tuple of a table in no particular order (e.g., as they are laid out on
 * disk).
 */
public class SeqScan implements DbIterator {
    private TransactionId transactionId;
    private int tableId;
    private String tableAlias;
    private DbFileIterator iterator;
    private DbFile file;


    /**
     * Creates a sequential scan over the specified table as a part of the
     * specified transaction.
     *
     * @param tid The transaction this scan is running as a part of.
     * @param tableid the table to scan.
     * @param tableAlias the alias of this table (needed by the parser);
     *         the returned tupleDesc should have fields with name tableAlias.fieldName
     *         (note: this class is not responsible for handling a case where tableAlias
     *         or fieldName are null.  It shouldn't crash if they are, but the resulting
     *         name can be null.fieldName, tableAlias.null, or null.null).
     */
    public SeqScan(TransactionId tid, int tableid, String tableAlias) {
        transactionId = tid;
        tableId = tableid;
        tableAlias = tableAlias;
        file = Database.getCatalog().getDatabaseFile(tableId);
        assert (file != null);
    }


    public SeqScan(TransactionId tid, int tableid) {
	this(tid, tableid, Database.getCatalog().getTableName(tableid));
    }


    public void open()
        throws DbException, TransactionAbortedException {
         HeapFile heapFile = (HeapFile) file;
         assert (heapFile != null);
         iterator = new HeapFileIterator(transactionId, heapFile);
         iterator.open();
    }


    /**
     * Returns the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor. This prefix
     * becomes useful when joining tables containing a field(s) with the same
     * name.
     * @return the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor.
     */
    public TupleDesc getTupleDesc() {
    	assert (file != null);
        TupleDesc fileDesc = file.getTupleDesc();
        int length = fileDesc.numFields();
        
        Type[] types = new Type[length];
        String[] names = new String[length];
        for (int i = 0; i < length; i++) {
            types[i] = fileDesc.getFieldType(i);
            // Table Alias Requires a table.field name
            names[i] = tableAlias + "." + fileDesc.getFieldName(i);
        }


        return new TupleDesc(types, names);
    }


    public boolean hasNext() throws TransactionAbortedException, DbException {
        return iterator.hasNext();
    }


    public Tuple next()
        throws NoSuchElementException, TransactionAbortedException, DbException {
        return iterator.next();
    }


    public void close() {
         iterator.close();
    }


    public void rewind()
        throws DbException, NoSuchElementException, TransactionAbortedException {
        iterator.rewind();
    }
}

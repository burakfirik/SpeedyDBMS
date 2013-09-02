package simpledb;

import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableid specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    private DbIterator child;
	private int tableId;
	private TransactionId tid;
	private TupleDesc resultDesc;
	private boolean didInsert;

    
    /**
     * Constructor.
     * 
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableid
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t,DbIterator child, int tableid)
            throws DbException {
        // some code goes here
    	tid = t;
    	this.child = child;
    	this.tableId = tableid;
    	createResultTupleDesc();

    }
    


    private void createResultTupleDesc() {
    	Type[] types = new Type[1];
    	types[0] = Type.INT_TYPE;
    	
    	String[] names = new String[1];
    	names[0] = "InsertCount";
    	resultDesc = new TupleDesc(types, names);
    }


    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.resultDesc;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
    	child.open();
    	didInsert = false;

    }

    public void close() {
        // some code goes here
    	this.child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
    	this.child.rewind();
    }

    /**
     * Inserts tuples read from child into the tableid specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     * 
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
if (this.didInsert) return null;
    	
    	BufferPool pool = Database.getBufferPool();
    	
    	int count = 0;
    	try {
    		while (this.child.hasNext()) {
    			Tuple next = this.child.next();
    			count++;
    			pool.insertTuple(this.tid, this.tableId, next);
    		}
    	} catch (IOException e) {
    		System.out.println("Error inserting tuple");
    		e.printStackTrace();
    		System.out.println(e.getMessage());
    		System.exit(1);
    	}
    	
    	Tuple result = new Tuple(this.resultDesc);
    	IntField intField = new IntField(count);
    	result.setField(0, intField);
    	this.didInsert = true;
    	return result;

    }

    @Override
    public DbIterator[] getChildren() {
        // some code goes here
        return null;
    }

    @Override
    public void setChildren(DbIterator[] children) {
        // some code goes here
    }
}

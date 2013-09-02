package simpledb;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;
    
    private TransactionId tid;
	private DbIterator child;
	private TupleDesc resultTupleDesc;
	private boolean didDelete;


    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, DbIterator child) {
        // some code goes here
    	this.tid = t;
    	this.child = child;
    	createResultTupleDesc();
    	this.didDelete = false;

    }

    private void createResultTupleDesc() {
		// TODO Auto-generated method stub
    	Type[] types = new Type[1];
    	types[0] = Type.INT_TYPE;
    	String[] names = new String[1];
    	names[0] = "DeleteCount";
    	
    	this.resultTupleDesc = new TupleDesc(types, names);

		
	}

	public TupleDesc getTupleDesc() {
        // some code goes here
        return this.resultTupleDesc;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
    	child.open();
    	this.didDelete = false;

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
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
if (this.didDelete) return null;
    	
    	Tuple result = new Tuple(getTupleDesc());
    	BufferPool pool = Database.getBufferPool();
    	int count = 0;
    	
    	while (this.child.hasNext()) {
    		Tuple next = this.child.next();
    		pool.deleteTuple(this.tid, next);
    		count++;
    	}


    	IntField resultField = new IntField(count);
    	result.setField(0, resultField);
    	this.didDelete = true;
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

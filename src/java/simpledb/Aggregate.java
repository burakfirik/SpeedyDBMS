package simpledb;


import java.util.*;


import simpledb.Aggregator.Op;


/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min).  Note that we only support aggregates over a single column, grouped
 * by a single column.
 */
public class Aggregate extends Operator {
	
	private static final long serialVersionUID = 1L;
	private DbIterator child;
	private int aggregateField;
	private int groupBy;
	private Aggregator.Op op;
	private Aggregator aggregator;
	private DbIterator results;


    /**
     * Constructor.  
     *
     *  Implementation hint: depending on the type of afield, you will want to construct an 
     *  IntAggregator or StringAggregator to help you with your implementation of readNext().
     * 
     *
     * @param child The DbIterator that is feeding us tuples.
     * @param afield The column over which we are computing an aggregate.
     * @param gfield The column over which we are grouping the result, or -1 if there is no grouping
     * @param aop The aggregation operator to use
     */
    public Aggregate(DbIterator child, int afield, int gfield, Aggregator.Op aop) {
    	
        this.child = child;
        this.aggregateField = afield;
        this.groupBy = gfield;
        this.op = aop;
        
        createAggregator();
    }

    
    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     *         field index in the <b>INPUT</b> tuples. If not, return
     *         {@link simpledb.Aggregator#NO_GROUPING}
     * */
    public int groupField() {
	// some code goes here
    	return this.groupBy;
    }
    

    /**
     * @return the aggregate field
     * */
    public int aggregateField() {
	// some code goes here
	return this.aggregateField;
    }
    
    
    /**
     * @return return the aggregate operator
     * */
    public Aggregator.Op aggregateOp() {
	// some code goes here
	return this.op;
    }




    private void createAggregator() {
    	TupleDesc desc = child.getTupleDesc();
    	
    	Type aggregateType = desc.getFieldType(aggregateField);
    	Type groupType = null;
    	
    	if (groupBy != Aggregator.NO_GROUPING) {
    		groupType = desc.getFieldType(groupBy);
    	}
    	
    	if (aggregateType == Type.INT_TYPE) {
    		aggregator = new IntegerAggregator(groupBy, groupType, aggregateField, op);
    	} else {
    		assert (aggregateType == Type.STRING_TYPE);
    		aggregator = new StringAggregator(groupBy, groupType, aggregateField, op);
    	}
	}


	public static String nameOfAggregatorOp(Aggregator.Op aop) {
        switch (aop) {
        case MIN:
            return "min";
        case MAX:
            return "max";
        case AVG:
            return "avg";
        case SUM:
            return "sum";
        case COUNT:
            return "count";
        }
        return "";
    }


    public void open()
        throws NoSuchElementException, DbException, TransactionAbortedException {
        this.child.open();
        while (this.child.hasNext()) {
        	Tuple t = child.next();
        	aggregator.mergeTupleIntoGroup(t);
        }
        
        results = aggregator.iterator();
        results.open();
    }


    /**
     * Returns the next tuple.  If there is a group by field, then 
     * the first field is the field by which we are
     * grouping, and the second field is the result of computing the aggregate,
     * If there is no group by field, then the result tuple should contain
     * one field representing the result of the aggregate.
     * Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
    	if (results.hasNext()) {
    		return results.next();
    	}
    	
    	return null;
    }


    public void rewind() throws DbException, TransactionAbortedException {
    	results.rewind();
    }


    /**
     * Returns the TupleDesc of this Aggregate.
     * If there is no group by field, this will have one field - the aggregate column.
     * If there is a group by field, the first field will be the group by field, and the second
     * will be the aggregate value column.
     * 
     * The name of an aggregate column should be informative.  For example:
     * "aggName(aop) (child_td.getFieldName(afield))"
     * where aop and afield are given in the constructor, and child_td is the TupleDesc
     * of the child iterator. 
     */
    public TupleDesc getTupleDesc() {
    	return aggregator.iterator().getTupleDesc();
    }


    public void close() {
    	results.close();
    }


	@Override
	public void setChildren(DbIterator[] children) {
		// TODO Auto-generated method stub
		
	}


	@Override
	public DbIterator[] getChildren() {
		// TODO Auto-generated method stub
		return null;
	}
}

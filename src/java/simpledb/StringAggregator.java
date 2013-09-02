package simpledb;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private HashMap<Integer, Integer> countData;
	private int groupBy;
	private Op op;
	private int aggregateField;

    
    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
    	this.groupBy = gbfield;
        this.aggregateField = afield;
        this.op = what;
        this.countData = new HashMap<Integer, Integer>();

    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
    	
    	IntField field = (IntField) tup.getField(this.groupBy);
        int key = field.getValue();
        
        if (!this.countData.containsKey(key)) {
        	this.countData.put(key, 0);
        }
        
        // TODO: Check for duplicate strings?
        int currentCount = this.countData.get(key);
        currentCount++;
        this.countData.put(key, currentCount);

    }

    /**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public DbIterator iterator() {
        // some code goes here
    	Type[] types = new Type[2];
    	types[0] = Type.INT_TYPE;
    	types[1] = Type.INT_TYPE;
    	
    	String[] names = new String[2];
    	names[0] = "key";
    	names[0] = this.op.toString();
    	TupleDesc description = new TupleDesc(types, names);
    	
    	ArrayList<Tuple> results = new ArrayList<Tuple>();
    	for (int key : this.countData.keySet()) {
    		int value = this.countData.get(key);
    		Tuple newTuple = new Tuple(description);
    		Field groupBy = new IntField(key);
    		Field aggregate = new IntField(value);
    		newTuple.setField(0, groupBy);
    		newTuple.setField(1, aggregate);
    		results.add(newTuple);
    	}
    	
    	return new TupleIterator(description, results);

    }

}

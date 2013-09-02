package simpledb;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;


/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {


	private int groupField;
	private Type fieldType;
	private int aggregateField;
	private Op op;
	private HashMap<Object, Integer> data;
	private HashMap<Object, Integer> keyCount;




    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what the aggregation operator
     */


    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        this.groupField = gbfield;
        this.fieldType = gbfieldtype;
        this.aggregateField = afield;
        this.op = what;
        this.data = new HashMap<Object, Integer>();
        this.keyCount = new HashMap<Object, Integer>();
    }
    
    private int getInitValue() {
    	switch (this.op) {
    	case AVG: return 0;
    	case COUNT: return 0;
    	case SUM: return 0;
    	case MIN: return Integer.MAX_VALUE;
    	case MAX: return Integer.MIN_VALUE;
    	default: assert (false);
    	}
    	
    	return 0;
    }
    
    private int getCurrentValue(Object key) {
    	if (!this.data.containsKey(key)) {
    		int defaultValue = getInitValue();
    		this.data.put(key, defaultValue);
    		this.keyCount.put(key, 0);
    	}
    	
    	return this.data.get(key);
    }
    
    private boolean isInteger(Field field) {
    	return field.getType() == Type.INT_TYPE;
    }
    
    private Object getKey(Tuple tuple) {
    	if (this.groupField == Aggregator.NO_GROUPING) return null;
    	
    	Field field = tuple.getField(this.groupField);
    	Type fieldType = field.getType();
    	assert (fieldType == this.fieldType);
    	if (isInteger(field)) {
    		return ((IntField) field).getValue();
    	} else {
    		return ((StringField) field).getValue();
    	}
    }
   
    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
    	Object key = getKey(tup);
    	int value = ((IntField) tup.getField(this.aggregateField)).getValue();
    	int aggregateValue = getCurrentValue(key);
    	
    	switch (this.op) {
    	case SUM:
    	{
    		aggregateValue += value;
    		break;
    	}
    	case MIN:
    	{
    		if (aggregateValue > value) {
    			aggregateValue = value;
    		}
    		break;
    	}
    	case MAX:
    	{
    		if (aggregateValue < value) {
    			aggregateValue = value;
    		}
    		break;
    	}
    	case AVG:
    	{
    		aggregateValue += value;


    		int keyCount = this.keyCount.get(key);
    		keyCount++;
    		this.keyCount.put(key,  keyCount);
    		break;
    	}
    	case COUNT:
    	{
    		aggregateValue++;
    		break;
    	}
    	default:
    		assert (false);
    	}
    	
    	this.data.put(key,  aggregateValue);
    }
    
    private boolean hasAggregate() {
    	return this.groupField != Aggregator.NO_GROUPING;
    }
    
    private Field getGroupField(Object key, TupleDesc description) {
    	if (!hasAggregate()) {
    		return new IntField(0);
    	} else if (this.fieldType == Type.INT_TYPE) {
    		assert (key instanceof Integer);
    		return new IntField((Integer) key);
		} else {
			assert (key instanceof String);
			return new StringField((String) key, this.fieldType.getLen());
		}
    }
    
    private TupleDesc createTupleDesc() {
    	if (hasAggregate()) {
    		Type[] types = new Type[2];
    		types[0] = this.fieldType;    	
        	types[1] = Type.INT_TYPE;
        	
        	String[] names = new String[2];
        	names[0] = "key";
        	names[1] = this.op.toString();
        	return new TupleDesc(types, names);
    	} else {
    		Type[] types = new Type[1];
    		types[0] = Type.INT_TYPE;
    		return new TupleDesc(types);
    	}
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
    	TupleDesc description = createTupleDesc();
    	ArrayList<Tuple> results = new ArrayList<Tuple>();
    	
    	for (Object key : this.data.keySet()) {
    		int value = this.data.get(key);
    		
    		Tuple newTuple = new Tuple(description);
    		Field groupBy = getGroupField(key, description);
    		if (this.op == Aggregator.Op.AVG) {
    			value = value / this.keyCount.get(key);
    		}
    		
    		Field aggregate = new IntField(value);
    		if (hasAggregate()) {
    			newTuple.setField(0, groupBy);
    			newTuple.setField(1, aggregate);
    		} else {
    			newTuple.setField(0, aggregate);
    		}
    		
    		results.add(newTuple);
    	}
    	
    	return new TupleIterator(description, results);
    }


}

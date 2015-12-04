package simpledb;

import java.util.ArrayList;
import java.util.Enumeration;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    
    // Arbitrary key for use when there is no grouping.
    private static final Field NO_GROUPING_KEY = new IntField(0);

    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;
    private ConcurrentHashMap<Field, Integer> values;
    
    // This is only used for AVG aggregation.
    private ConcurrentHashMap<Field, Integer> count;
    
    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */
    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        this.values = new ConcurrentHashMap<Field, Integer>();
        this.count = new ConcurrentHashMap<Field, Integer>();
        
        if (this.gbfield == Aggregator.NO_GROUPING) {
        	this.values.put(NO_GROUPING_KEY, 0);
        	this.count.put(NO_GROUPING_KEY, 0);
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
    	Field key = NO_GROUPING_KEY;
    	if (this.gbfield != Aggregator.NO_GROUPING) {
    		key = tup.getField(this.gbfield);
    	}
    	
    	int value = ((IntField) (tup.getField(this.afield))).getValue();
    	
    	// Only merge tuple if field matches.
    	if (this.gbfield == Aggregator.NO_GROUPING ||
    			tup.getTupleDesc().getFieldType(this.gbfield)
    								  .equals(this.gbfieldtype)) {
    		if (!(this.values.containsKey(key))) {
    			this.values.put(key, value);
    			this.count.put(key, 1);
    		} else {
	    		this.values.put(
	    			key,
	    			this.combine(this.values.get(key), value));
	    		this.count.put(
	    			key,
	    			this.count.get(key) + 1);
    		}
    	}
    }
    
    /**
     * Helper function for applying the operator. a represents the previously
     * computed value and b represents the new value. AVG and SUM return the
     * same value as the average is computed after all of the values have been
     * aggregated. COUNT only depends on the previous value a as it simply
     * increments the counter.
     */
    private int combine(int a, int b) {
        switch (what) {
	        case MIN:
	            return Math.min(a, b);
	        case MAX:
	            return Math.max(a, b);
	
	        // These return the same thing as AVG is computed first as a sum
	        // and divided by the number of elements at the end.
	        case SUM:
	        case AVG:
	            return a + b;
	
	        // This only depends on the previous value, which is a.
	        case COUNT:
	            return a + 1;
	
	        // These two are currently unimplemented.
	        case SUM_COUNT:
	            return 0;
	
		    case SC_AVG:
		        return 0;
        }
        
        throw new IllegalStateException(
        	"Aggregation operator not recognized: " + this.what.toString());
    }
    
    /**
     * Create a DbIterator over group aggregate results.
     * 
     * @return a DbIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public DbIterator iterator() {
    	TupleDesc td;
    	ArrayList<Tuple> tuples = new ArrayList<Tuple>();
    	
    	if (this.gbfield == Aggregator.NO_GROUPING) {
    		td = new TupleDesc(new Type[] { Type.INT_TYPE });
    		
    		Tuple t = new Tuple(td);
    		
    		int value = this.values.get(NO_GROUPING_KEY);
    		if (this.what == Aggregator.Op.AVG) {
    			value /= this.count.get(NO_GROUPING_KEY);
    		} else if (this.what == Aggregator.Op.COUNT) {
    			value = this.count.get(NO_GROUPING_KEY);
    		}
    		
    		t.setField(0, new IntField(value));
    		tuples.add(t);
    	} else {
    		td = new TupleDesc(new Type[] { this.gbfieldtype, Type.INT_TYPE });
	    	Enumeration<Field> keys = this.values.keys();
	    	
	    	while (keys.hasMoreElements()) {
	    		Tuple t = new Tuple(td);
	    		Field key = keys.nextElement();
	    		int value = this.values.get(key);
	    		if (this.what == Aggregator.Op.AVG) {
	    			value /= this.count.get(key);
	    		} else if (this.what == Aggregator.Op.COUNT) {
	    			value = this.count.get(key);
	    		}
	    		
	    		t.setField(0, key);
	    		t.setField(1, new IntField(value));
	    		tuples.add(t);
	    	}
    	}
    	
        return new TupleIterator(td, tuples);
    }

}

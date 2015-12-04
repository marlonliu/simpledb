package simpledb;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * Tuple maintains information about the contents of a tuple. Tuples have a
 * specified schema specified by a TupleDesc object and contain Field objects
 * with the data for each field.
 */
public class Tuple implements Serializable {

    private static final long serialVersionUID = 1L;
    
    private ArrayList<Field> fields;
    private TupleDesc td;
    private RecordId rid = null;

    /**
     * Create a new tuple with the specified schema (type).
     * 
     * @param td
     *            the schema of this tuple. It must be a valid TupleDesc
     *            instance with at least one field.
     */
    public Tuple(TupleDesc td) {
    	this.fields = new ArrayList<Field>();
    	
    	// Initialize IntFields to 0 and StringFields to "".
    	for (int i = 0; i < td.numFields(); i++) {
    		if (td.getFieldType(i) == Type.INT_TYPE) {
    			this.fields.add(new IntField(0));
    		} else if (td.getFieldType(i) == Type.STRING_TYPE) {
    			this.fields.add(new StringField("", 0));
    		}
    	}
    	
    	this.td = td;
    }

    /**
     * @return The TupleDesc representing the schema of this tuple.
     */
    public TupleDesc getTupleDesc() {
        return this.td;
    }

    /**
     * @return The RecordId representing the location of this tuple on disk. May
     *         be null.
     */
    public RecordId getRecordId() {
        return this.rid;
    }

    /**
     * Set the RecordId information for this tuple.
     * 
     * @param rid
     *            the new RecordId for this tuple.
     */
    public void setRecordId(RecordId rid) {
        this.rid = rid;
    }

    /**
     * Change the value of the ith field of this tuple.
     * 
     * @param i
     *            index of the field to change. It must be a valid index.
     * @param f
     *            new value for the field.
     */
    public void setField(int i, Field f) {
    	// Only set the field if it has the correct type.
        if (f.getType() == this.td.getFieldType(i)) {
        	this.fields.set(i, f);
        }
    }

    /**
     * @return the value of the ith field, or null if it has not been set.
     * 
     * @param i
     *            field index to return. Must be a valid index.
     */
    public Field getField(int i) {
        return this.fields.get(i);
    }

    /**
     * Returns the contents of this Tuple as a string. Note that to pass the
     * system tests, the format needs to be as follows:
     * 
     * column1\tcolumn2\tcolumn3\t...\tcolumnN\n
     * 
     * where \t is any whitespace, except newline, and \n is a newline
     */
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < this.fields.size() - 1; i++) {
        	sb.append(this.fields.get(i).toString());
        	sb.append("\t");
        }
        
        sb.append(this.fields.get(this.fields.size() - 1).toString());
        
        return sb.toString();
    }
    
    /**
     * @return
     *        An iterator which iterates over all the fields of this tuple
     * */
    public Iterator<Field> fields()
    {
        return this.fields.iterator();
    }
    
    /**
     * reset the TupleDesc of this tuple
     * */
    public void resetTupleDesc(TupleDesc td)
    {
    	// TODO: Not exactly sure what reset means.
        td = new TupleDesc(new Type[] {}, new String[] {});
    }
}

package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

	private ArrayList<TDItem> fieldDescs;

    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public final Type fieldType;
        
        /**
         * The name of the field
         * */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        return fieldDescs.iterator();
    }

    private static final long serialVersionUID = 1L;
    
    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        this.fieldDescs = new ArrayList<TDItem>();
        for (int i = 0; i < typeAr.length; i++) {
        	this.fieldDescs.add(new TDItem(typeAr[i], fieldAr[i]));
        }
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        this.fieldDescs = new ArrayList<TDItem>();
        for (int i = 0; i < typeAr.length; i++) {
        	this.fieldDescs.add(new TDItem(typeAr[i], null));
        }
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        return this.fieldDescs.size();
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
    	if (i < 0 || i  >= this.numFields()) {
    		throw new NoSuchElementException();
    	}

        return this.fieldDescs.get(i).fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
    	if (i < 0 || i  >= this.numFields()) {
    		throw new NoSuchElementException();
    	}

        return this.fieldDescs.get(i).fieldType;
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
    	if (name == null) {
    		throw new NoSuchElementException();
    	}

    	int index = 0;
    	Iterator<TDItem> it = this.iterator();
        while (it.hasNext()) {
        	TDItem item = it.next();
        	if (item.fieldName != null && item.fieldName.equals(name)) {
        		return index;
        	}
        	index++;
        }

        throw new NoSuchElementException();
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
    	int size = 0;
        Iterator<TDItem> it = this.iterator();
        while (it.hasNext()) {
        	size += it.next().fieldType.getLen();
        }
        
        return size;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        // Variables for getting rid of extraneous function calls.
        int td1Size = td1.numFields();
        int td2Size = td2.numFields();

        Type[] types = new Type[td1Size + td2Size];
        String[] names = new String[td1Size + td2Size];
        
        // Add all the items from td1.
        for (int i = 0; i < td1Size; i++) {
        	types[i] = td1.getFieldType(i);
        	names[i] = td1.getFieldName(i);
        }
        
        // Add all the items from td2.
        for (int i = 0; i < td2Size; i++) {
        	types[i + td1Size] = td2.getFieldType(i);
        	names[i + td1Size] = td2.getFieldName(i);
        }
        
        return new TupleDesc(types, names);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they are the same size and if the n-th
     * type in this TupleDesc is equal to the n-th type in td.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */
    public boolean equals(Object o) {
        // Check if the object is valid.
    	if (o == null || !(o instanceof TupleDesc)) {
        	return false;
        }
        
    	// Cast the object.
        TupleDesc td = (TupleDesc) o;
        
        // Check that each TupleDesc has the same number of fields.
        if (this.numFields() != td.numFields()) {
        	return false;
        }
        
        Iterator<TDItem> thisIter = this.iterator();
        Iterator<TDItem> otherIter = td.iterator();
        
        // Check that each field has the same type.
        while (thisIter.hasNext() && otherIter.hasNext()) {
        	if (thisIter.next().fieldType != otherIter.next().fieldType) {
        		return false;
        	}
        }
        
        return true;
    }

    public int hashCode() {
        return Arrays.hashCode(this.fieldDescs.toArray());
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        StringBuilder sb = new StringBuilder();
        Iterator<TDItem> it = iterator();
        while (it.hasNext()) {
        	sb.append(it.next().toString() + ", ");
        }

        if (sb.length() > 2) {
        	sb.delete(sb.length() - 2, sb.length());
        }

        return sb.toString();
    }
}

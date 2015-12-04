package simpledb;

import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableid specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;
    
    private TransactionId t;
    private DbIterator child;
    private int tableid;
    private boolean fetched = false;
    private TupleDesc td = new TupleDesc(new Type[] { Type.INT_TYPE });
    
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
    public Insert(TransactionId t, DbIterator child, int tableid)
            throws DbException {
        if (!(Database
        		  .getCatalog()
        		  .getTupleDesc(tableid)
        		  .equals(child.getTupleDesc()))) {
        	throw new DbException("TupleDesc from table and of child are different.");
        }
        
        this.t = t;
        this.child = child;
        this.tableid = tableid;
    }

    public TupleDesc getTupleDesc() {
        return this.td;
    }

    public void open() throws DbException, TransactionAbortedException {
        super.open();
    }

    public void close() {
        super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
    	// Do nothing.
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
        if (this.fetched) {
        	return null;
        }
        
        int numInserted = 0;
        try {
        	this.child.open();
        	
	        while (this.child.hasNext()) {
	        	Database.getBufferPool().insertTuple(this.t, this.tableid, child.next());
	        	numInserted++;
	        }
	        
	        this.child.close();
        } catch(IOException e) {
        	e.printStackTrace();
        }
        
        this.fetched = true;
        
        Tuple t = new Tuple(this.td);
        t.setField(0, new IntField(numInserted));
        
        return t;
    }

    @Override
    public DbIterator[] getChildren() {
        return new DbIterator[] { this.child };
    }

    @Override
    public void setChildren(DbIterator[] children) {
    	if (children.length < 1) {
    		throw new IllegalArgumentException(
    			"children has the wrong number of elements.");
    	}
    	
        this.child = children[0];
    }
}

package simpledb;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;

    private TransactionId t;
    private DbIterator child;
    private boolean fetched = false;
    private TupleDesc td = new TupleDesc(new Type[] { Type.INT_TYPE });
    
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
        this.t = t;
        this.child = child;
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
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if (this.fetched) {
        	return null;
        }
        
    	int numDeleted = 0;
        try {
        	this.child.open();
        	
	        while (this.child.hasNext()) {
	        	Database.getBufferPool().deleteTuple(this.t, this.child.next());
	        	numDeleted++;
	        }
	        
	        this.child.close();
        } catch(IOException e) {
        	e.printStackTrace();
        }
        
        this.fetched = true;
        
        Tuple t = new Tuple(this.td);
        t.setField(0, new IntField(numDeleted));
        
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

package simpledb;

import java.util.*;

/**
 * SeqScan is an implementation of a sequential scan access method that reads
 * each tuple of a table in no particular order (e.g., as they are laid out on
 * disk).
 */
public class SeqScan implements DbIterator {

    private static final long serialVersionUID = 1L;
    
    private TransactionId tid;
    private int tableid;
    private String tableAlias;
    private DbFileIterator it;
    
    /**
     * Creates a sequential scan over the specified table as a part of the
     * specified transaction.
     * 
     * @param tid
     *            The transaction this scan is running as a part of.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public SeqScan(TransactionId tid, int tableid, String tableAlias) {
        this.tid = tid;
        this.tableid = tableid;
        this.tableAlias = tableAlias;
        this.it = Database
        			  .getCatalog()
        			  .getDatabaseFile(this.tableid)
        			  .iterator(this.tid);
    }

    /**
     * @return
     *       return the table name of the table the operator scans. This should
     *       be the actual name of the table in the catalog of the database
     * */
    public String getTableName() {
        return Database.getCatalog().getTableName(this.tableid);
    }
    
    /**
     * @return Return the alias of the table this operator scans. 
     * */
    public String getAlias()
    {
        return this.tableAlias;
    }

    /**
     * Reset the tableid, and tableAlias of this operator.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public void reset(int tableid, String tableAlias) {
        this.tableid = tableid;
        this.tableAlias = tableAlias;
    }

    public SeqScan(TransactionId tid, int tableid) {
        this(tid, tableid, Database.getCatalog().getTableName(tableid));
    }

    public void open() throws DbException, TransactionAbortedException {
        this.it.open();
    }

    /**
     * Returns the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor. This prefix
     * becomes useful when joining tables containing a field(s) with the same
     * name.
     * 
     * @return the TupleDesc with field names from the underlying HeapFile,
     *         prefixed with the tableAlias string from the constructor.
     */
    public TupleDesc getTupleDesc() {
    	String alias = this.tableAlias;
    	if (alias == null) {
    		alias = "null";
    	}

        TupleDesc td =  Database
				        	  .getCatalog()
				        	  .getDatabaseFile(this.tableid)
				        	  .getTupleDesc();

        Type[] types = new Type[td.numFields()];
        String[] fields = new String[td.numFields()];

        for (int i = 0; i < td.numFields(); i++) {
        	types[i] = td.getFieldType(i);

        	String fieldName = td.getFieldName(i);
        	if (fieldName == null) {
        		fieldName = "null";
        	}

        	fields[i] = alias + "." + fieldName;
        }

        return new TupleDesc(types, fields);
    }

    public boolean hasNext() throws TransactionAbortedException, DbException {
        return this.it.hasNext();
    }

    public Tuple next() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        return this.it.next();
    }

    public void close() {
        this.it.close();
    }

    public void rewind() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        this.it.rewind();
    }
}

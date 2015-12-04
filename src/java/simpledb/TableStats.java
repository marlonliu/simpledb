package simpledb;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;

import simpledb.TupleDesc.TDItem;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query. 
 * 
 * This class is not needed in implementing lab1 and lab2.
 */
public class TableStats {

    private static final ConcurrentHashMap<String, TableStats> statsMap = new ConcurrentHashMap<String, TableStats>();

    static final int IOCOSTPERPAGE = 1000;

    public static TableStats getTableStats(String tablename) {
        return statsMap.get(tablename);
    }

    public static void setTableStats(String tablename, TableStats stats) {
        statsMap.put(tablename, stats);
    }
    
    public static void setStatsMap(HashMap<String,TableStats> s)
    {
        try {
            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
            statsMapF.setAccessible(true);
            statsMapF.set(null, s);
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        } catch (SecurityException e) {
            e.printStackTrace();
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }

    }

    public static Map<String, TableStats> getStatsMap() {
        return statsMap;
    }

    public static void computeStatistics() {
        Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

        System.out.println("Computing table stats.");
        while (tableIt.hasNext()) {
            int tableid = tableIt.next();
            TableStats s = new TableStats(tableid, IOCOSTPERPAGE);
            setTableStats(Database.getCatalog().getTableName(tableid), s);
        }
        System.out.println("Done.");
    }

    /**
     * Number of bins for the histogram. Feel free to increase this value over
     * 100, though our tests assume that you have at least 100 bins in your
     * histograms.
     */
    static final int NUM_HIST_BINS = 100;

    private int tableid;
    private int ioCostPerPage;
    private int total;
    private ConcurrentHashMap<Integer, IntHistogram> intHists;
    private ConcurrentHashMap<Integer, StringHistogram> stringHists;
    private DbFile file;

    /**
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     * 
     * @param tableid
     *            The table over which to compute statistics
     * @param ioCostPerPage
     *            The cost per page of IO. This doesn't differentiate between
     *            sequential-scan IO and disk seeks.
     */
    public TableStats(int tableid, int ioCostPerPage) {
    	this.tableid = tableid;
    	this.ioCostPerPage = ioCostPerPage;
    	this.total = 0;
    	this.intHists = new ConcurrentHashMap<Integer, IntHistogram>();
    	this.stringHists = new ConcurrentHashMap<Integer, StringHistogram>();
    	this.file = Database.getCatalog().getDatabaseFile(this.tableid);

    	try {
	    	TupleDesc td = this.file.getTupleDesc();
	
	    	// Get the mins and the maxes.
	    	int[] mins = new int[td.numFields()];
	    	int[] maxes = new int[td.numFields()];
	    	
	    	TransactionId tid = new TransactionId();
	    	DbFileIterator dbIter = file.iterator(tid);
	    	dbIter.open();
			while (dbIter.hasNext()) {
				Tuple t = dbIter.next();
				Iterator<Field> it = t.fields();
				for (int i = 0; i < td.numFields(); i++) {
					Field f = it.next();
					if (f.getType() == Type.INT_TYPE) {
						int value = ((IntField) f).getValue();
						if (value > maxes[i]) {
							maxes[i] = value;
						}
	
						if (value < mins[i]) {
							mins[i] = value;
						}
					}
				}

				this.total++;
			}
	
	    	// Instantiate the histograms.
	    	Iterator<TDItem> tdIter = td.iterator();
	    	for (int i = 0; i < td.numFields(); i++) {
	    		if (tdIter.next().fieldType == Type.INT_TYPE) {
	    			// Use at most NUM_HIST_BINS.
	    			this.intHists.put(
	    					 i,
	    					 new IntHistogram(
	    							 Math.min(
	    									  NUM_HIST_BINS,
	    									  maxes[i] - mins[i] + 1),
	    							 mins[i],
	    							 maxes[i]));
	    		} else {
	    			this.stringHists.put(i, new StringHistogram(
	    											NUM_HIST_BINS));
	    		}
	    	}
	
	    	// Add all the tuple fields to the corresponding histograms.
			dbIter.rewind();
			while (dbIter.hasNext()) {
				Tuple t = dbIter.next();
				Iterator<Field> it = t.fields();
				for (int i = 0; i < td.numFields(); i++) {
					Field f = it.next();
					if (f.getType() == Type.INT_TYPE) {
						this.intHists.get(i).addValue(
												((IntField) f).getValue());
					} else {
						this.stringHists.get(i)
											.addValue(
												((StringField) f).getValue());
					}
				}
			}

			dbIter.close();
    	} catch (NoSuchElementException e) {
    		e.printStackTrace();
    	} catch (TransactionAbortedException e) {
    		e.printStackTrace();
    	} catch (DbException e) {
			e.printStackTrace();
		}
    }

    /**
     * Estimates the cost of sequentially scanning the file, given that the cost
     * to read a page is costPerPageIO. You can assume that there are no seeks
     * and that no pages are in the buffer pool.
     * 
     * Also, assume that your hard drive can only read entire pages at once, so
     * if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page. (Most real hard drives can't
     * efficiently address regions smaller than a page at a time.)
     * 
     * @return The estimated cost of scanning the table.
     */
    public double estimateScanCost() {
    	double numPages =
    		(1.0 * this.total) *
    		this.file.getTupleDesc().getSize() /
    		BufferPool.getPageSize();

		return numPages * this.ioCostPerPage;
    }

    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     * 
     * @param selectivityFactor
     *            The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified
     *         selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
        return (int) (selectivityFactor * this.total);
    }

    /**
     * The average selectivity of the field under op.
     * @param field
     *        the index of the field
     * @param op
     *        the operator in the predicate
     * The semantic of the method is that, given the table, and then given a
     * tuple, of which we do not know the value of the field, return the
     * expected selectivity. You may estimate this value from the histograms.
     * */
    public double avgSelectivity(int field, Predicate.Op op) {
        // some code goes here
        return 1.0;
    }

    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     * 
     * @param field
     *            The field over which the predicate ranges
     * @param op
     *            The logical operation in the predicate
     * @param constant
     *            The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     *         predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
        if (constant.getType() == Type.INT_TYPE) {
        	return this.intHists.get(field)
        							.estimateSelectivity(
        								op, ((IntField) constant).getValue());
        } else {
        	return this.stringHists.get(field)
        							   .estimateSelectivity(
        								   op,
        								   ((StringField) constant)
        								   	   .getValue());
        }
    }

    /**
     * return the total number of tuples in this table
     * */
    public int totalTuples() {
        return this.total;
    }

}

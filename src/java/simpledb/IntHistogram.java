package simpledb;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {
	private int total;
	private double[] keys;
	private int[] buckets;

    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
    	// Keys actually contains the max value as well.
    	this.keys = new double[buckets + 1];
    	this.buckets = new int[buckets];
    	this.total = 0;

    	// Initialize the histogram.
    	double step = (max - min + 1) / (1.0 * buckets);
    	double key = min;
    	for (int i = 0; i < buckets; i++) {
    		this.keys[i] = key;
    		this.buckets[i] = 0;
    		key += step;
    	}

    	this.keys[buckets] = max;
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	boolean added = false;
    	for (int i = 0; i < this.keys.length - 1; i++) {
    		if ((v >= this.keys[i]) && (v < this.keys[i + 1])) {
    			this.buckets[i]++;
    			added = true;
    			break;
    		}
    	}

    	if (!added) {
    		this.buckets[this.keys.length - 2]++;
    	}

    	this.total++;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
    	// Calculate the selectivity given the predicate.
    	double selectivity = 0;
    	if ((op == Predicate.Op.EQUALS) || op == Predicate.Op.NOT_EQUALS) {
			if (v > this.keys[this.keys.length - 1] || v < this.keys[0]) {
				selectivity = 0;
			} else {
	    		for (int i = 0; i < keys.length - 1; i++) {
	    			if ((v >= this.keys[i]) && (v < this.keys[i + 1])) {
	        			selectivity =
	        				(1.0 * this.buckets[i]) /
	        				(this.keys[i + 1] - this.keys[i]) /
	        				this.total;
	        			break;
	        		}
	    		}
			}

    		if (op == Predicate.Op.NOT_EQUALS) {
    			selectivity = 1 - selectivity;
    		}
    	} else if ((op == Predicate.Op.GREATER_THAN) ||
    				  (op == Predicate.Op.GREATER_THAN_OR_EQ)) {
        	if (v > this.keys[this.keys.length - 1]) {
        		selectivity = 0;
        	} else if (v < this.keys[0]) {
        		selectivity = 1;
        	} else {
        		int bucket = 0;
        		for (int i = 0; i < keys.length - 1; i++) {
        			if ((v >= this.keys[i]) && (v < this.keys[i + 1])) {
        				break;
        			}

        			bucket++;
        		}

        		// Set the selectivity to the entire selectivity of the
        		// containing bucket.
        		selectivity = (1.0 * this.buckets[bucket]) / total;

        		if (op == Predicate.Op.GREATER_THAN) {
	        		// Get the containing bucket's partial selectivity.
	        		selectivity *=
	        			(this.keys[bucket + 1] - v) /
	        			(this.keys[bucket + 1] - this.keys[bucket]);
        		}

        		// Add the entire selectivity of the remaining buckets.
        		for (int i = bucket + 1; i < keys.length - 1; i++) {
        			selectivity += (1.0 * this.buckets[i]) / total;
        		}
        	}
    	} else if ((op == Predicate.Op.LESS_THAN) ||
    				  (op == Predicate.Op.LESS_THAN_OR_EQ)) {
    		if (v > this.keys[this.keys.length - 1]) {
    			selectivity = 1;
    		} else if (v < this.keys[0]) {
    			selectivity = 0;
    		} else {
    			selectivity = 0;
    			for (int i = 0; i < keys.length - 1; i++) {
        			if ((v >= this.keys[i]) && (v < this.keys[i + 1])) {
        				double sel = (1.0 * this.buckets[i]) / total;

        				if (op == Predicate.Op.LESS_THAN) {
	        				// Add the containing bucket's partial selectivity.
	        				sel *= (v - this.keys[i]) /
	        						   (this.keys[i + 1] - this.keys[i]);
        				}

        				selectivity += sel;
        				break;
        			}

        			// Add the entire selectivity of the current bucket.
        			selectivity +=
            			(1.0 * this.buckets[i]) / total;
        		}
    		}
    	}

    	return selectivity;
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        // some code goes here
        return 1.0;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < this.keys.length - 1; i++) {
        	sb.append(this.keys[i] + ": " + this.buckets[i] + "\n");
        }

        return sb.toString();
    }
}

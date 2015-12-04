package simpledb;

import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.concurrent.ConcurrentHashMap;

public class Locker {
	
	// Used as a placeholder transaction for pages without locks.
	private static TransactionId NO_LOCK = new TransactionId();
	
	private ConcurrentHashMap<PageId, Object> locks;
	private ConcurrentHashMap<PageId, HashSet<TransactionId>> sharedLocks;
	private ConcurrentHashMap<PageId, TransactionId> exclusiveLocks;
	private ConcurrentHashMap<TransactionId, HashSet<PageId>> transactionPageMap;
	private ConcurrentHashMap<TransactionId, HashSet<TransactionId>> dependencyMap;

	public Locker() {
		this.locks = new ConcurrentHashMap<PageId, Object>();
		this.sharedLocks = new ConcurrentHashMap<PageId, HashSet<TransactionId>>();
		this.exclusiveLocks = new ConcurrentHashMap<PageId, TransactionId>();
		this.transactionPageMap = new ConcurrentHashMap<TransactionId, HashSet<PageId>>();
		this.dependencyMap = new ConcurrentHashMap<TransactionId, HashSet<TransactionId>>();
	}

	private Object getLock(PageId pid) {
		if (!(this.locks.containsKey(pid))) {
			this.locks.put(pid, new Object());
			this.sharedLocks.put(pid, new HashSet<TransactionId>());
			this.exclusiveLocks.put(pid, NO_LOCK);
		}

		return this.locks.get(pid);
	}

	// Run BFS to detect cycles.
	private boolean containsCycle(TransactionId tid) {
		HashSet<TransactionId> visited = new HashSet<TransactionId>();
		LinkedList<TransactionId> queue = new LinkedList<TransactionId>();

		queue.add(tid);

		while (!(queue.isEmpty())) {
			TransactionId cur = queue.remove();
			if (visited.contains(cur)) {
				return true;
			}

			visited.add(cur);

			if (this.dependencyMap.containsKey(cur) && !(this.dependencyMap.get(cur).isEmpty())) {
				Iterator<TransactionId> it = this.dependencyMap.get(cur).iterator();
				while (it.hasNext()) {
					queue.add(it.next());
				}
			}
		}

		return false;
	}

	public void acquireLock(TransactionId tid, PageId pid, Permissions p)
		throws TransactionAbortedException {

		if (!(this.dependencyMap.containsKey(tid))) {
			this.dependencyMap.put(tid, new HashSet<TransactionId>());
		}

		Object lock = this.getLock(pid);
		if ((p == Permissions.READ_ONLY) &&
				!(this.sharedLocks.get(pid).contains(tid))) {
			while (true) {
				synchronized(lock) {
					// Try to grab the lock.
					if (this.exclusiveLocks.get(pid).equals(NO_LOCK) ||
							this.exclusiveLocks.get(pid).equals(tid)) {
						synchronized(this.sharedLocks.get(pid)) {
							this.sharedLocks.get(pid).add(tid);
						}

						synchronized(this.dependencyMap) {
							this.dependencyMap.remove(tid);
						}

						break;
					}

					// Check for deadlock.
					synchronized(this.dependencyMap) {
						if (this.dependencyMap
								.get(tid)
								.add(this.exclusiveLocks.get(pid))) {
							if (this.containsCycle(tid)) {
								throw new TransactionAbortedException();
							}
						}
					}
				}
			}
		} else if ((p == Permissions.READ_WRITE) &&
					   !(this.exclusiveLocks.get(pid).equals(tid))) {
			while (true) {
				synchronized(lock) {
					// Gather the dependencies.
					HashSet<TransactionId> deps = new HashSet<TransactionId>();
					if (!(this.exclusiveLocks.get(pid).equals(NO_LOCK))) {
						deps.add(this.exclusiveLocks.get(pid));
					}

					synchronized(this.sharedLocks.get(pid)) {
						Iterator<TransactionId> it = this.sharedLocks.get(pid).iterator();
						while (it.hasNext()) {
							TransactionId t = it.next();
							if (!(t.equals(tid))) {
								deps.add(t);
							}
						}
					}

					// Try to grab the lock.
					if (deps.isEmpty()) {
						// Remove the shared lock if currently holding one.
						synchronized(this.sharedLocks.get(pid)) {
							this.sharedLocks.get(pid).remove(tid);
						}

						this.exclusiveLocks.put(pid, tid);

						synchronized(this.dependencyMap) {
							this.dependencyMap.remove(tid);
						}

						break;
					}

					// Check for deadlock.
					synchronized(this.dependencyMap) {
						if (this.dependencyMap
								.get(tid)
								.add(this.exclusiveLocks.get(pid)) ||
							this.dependencyMap.get(tid).addAll(deps)) {
	
							if (this.containsCycle(tid)) {
								throw new TransactionAbortedException();
							}
						}
					}
				}
			}
		}
		
		if (!(this.transactionPageMap.containsKey(tid))) {
			this.transactionPageMap.put(tid, new HashSet<PageId>());
		}
		
		synchronized(this.transactionPageMap.get(tid)) {
			this.transactionPageMap.get(tid).add(pid);
		}
	}

	public void releaseLock(TransactionId tid, PageId pid) {
		if (!(this.transactionPageMap.containsKey(tid))) {
			return;
		}

		Object lock = this.getLock(pid);
		synchronized(lock) {
			if (this.exclusiveLocks.get(pid).equals(tid)) {
				this.exclusiveLocks.put(pid, NO_LOCK);
			}
	
			synchronized(this.sharedLocks.get(pid)) {
				this.sharedLocks.get(pid).remove(tid);
			}
		}

		synchronized(this.transactionPageMap.get(tid)) {
			this.transactionPageMap.get(tid).remove(pid);
		}
	}

	public void releaseAllLocks(TransactionId tid) {
		if (!(this.transactionPageMap.containsKey(tid))) {
			return;
		}

		Iterator<PageId> it = this.getPages(tid);
		while (it.hasNext()) {
			PageId pid = it.next();

			Object lock = this.getLock(pid);
			synchronized(lock) {
				if (this.exclusiveLocks.get(pid).equals(tid)) {
					this.exclusiveLocks.put(pid, NO_LOCK);
				}
	
				synchronized(this.sharedLocks.get(pid)) {
					this.sharedLocks.get(pid).remove(tid);
				}
			}
		}

		this.transactionPageMap.remove(tid);
	}

	public Iterator<PageId> getPages(TransactionId tid) {
		if (!(this.transactionPageMap.containsKey(tid))) {
			return null;
		}

		return this.transactionPageMap.get(tid).iterator();
	}

	public boolean holdsLock(TransactionId tid, PageId pid) {
		if (!(this.transactionPageMap.containsKey(tid))) {
			return false;
		}

		synchronized(this.transactionPageMap.get(tid)) {
			return this.transactionPageMap.get(tid).contains(pid);
		}
	}
}

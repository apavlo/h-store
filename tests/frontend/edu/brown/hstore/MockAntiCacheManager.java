package edu.brown.hstore;

import org.voltdb.catalog.Table;

import edu.brown.hstore.txns.RemoteTransaction;

public class MockAntiCacheManager extends AntiCacheManager{

	protected MockAntiCacheManager(HStoreSite hstore_site) {
		super(hstore_site);
		// TODO Auto-generated constructor stub
	}
	
	public void processQueue(){
		QueueEntry next = null;
		try {
			next = this.queue.take();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		this.processingCallback(next);
	}

}

package edu.brown.benchmark.auctionmark;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestPartitionIdGenerator {

	private static final long MAXIMUM_ID = 100000000000000l;
	private static final int NUM_CLIENTS = 5;
	private PartitionIdGenerator _idGenerator;
	
	
	@Before
	public void setUp() throws Exception {
		_idGenerator = new PartitionIdGenerator(NUM_CLIENTS, 0, MAXIMUM_ID);
	}

	@Test
	public void test() throws Exception {
		
		long id = 0;
		for(long i=0; i<200; i++){
			for(long j=0; j<NUM_CLIENTS; j++){
				long expectedId = j * MAXIMUM_ID + id;
				long actualId = _idGenerator.getNextId();
				System.out.println(actualId);
				Assert.assertEquals("Id must be the same", expectedId, actualId);
			}
			id++;
		}
	}
}

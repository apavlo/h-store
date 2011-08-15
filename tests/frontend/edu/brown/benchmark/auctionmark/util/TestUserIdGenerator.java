package edu.brown.benchmark.auctionmark.util;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import edu.brown.benchmark.auctionmark.util.UserIdGenerator;

public class TestUserIdGenerator {

	private static final long MAXIMUM_ID = 100000000000000l;
	private static final int NUM_CLIENTS = 10;
	private UserIdGenerator _idGenerator;
	
	
	@Before
	public void setUp() throws Exception {
		// FIXME _idGenerator = new UserIdGenerator(NUM_CLIENTS, 0, MAXIMUM_ID);
	}

	@Test
	public void test() throws Exception {
		
//		long id = 0;
//		for(long i=0; i<200; i++){
//			for(long j=0; j<NUM_CLIENTS; j++){
//				long expectedId = j * MAXIMUM_ID + id;
//				long actualId = _idGenerator.next();
//				System.out.println(actualId);
//				Assert.assertEquals("Id must be the same", expectedId, actualId);
//			}
//			id++;
//		}
	}
}

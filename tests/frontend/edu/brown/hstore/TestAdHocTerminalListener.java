package edu.brown.hstore;

import static org.junit.Assert.*;

import java.io.IOException;

import org.junit.Before;
import org.junit.Test;

public class TestAdHocTerminalListener {
	 
	AdHocTerminalListener _term;
	
	@Before
	public void setUp() throws Exception {
		_term = new AdHocTerminalListener();
	 
	}
	 
	@Test
	public void testIO(){
		
		try {
			String q = _term.getQuery();
			System.out.println("Query was: "+q);
			assertTrue(q.length()>0);
			_term.printResult(q);
			
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

}

package edu.brown.benchmark.complexstreamtrigger;

import org.voltdb.VoltProcedure;

import edu.brown.benchmark.AbstractProjectBuilder;
import edu.brown.api.BenchmarkComponent;

import edu.brown.benchmark.complexstreamtrigger.procedures.Initialize; 
import edu.brown.benchmark.complexstreamtrigger.procedures.SimpleCall; 
import edu.brown.benchmark.complexstreamtrigger.procedures.ComplexTrigger1; 
import edu.brown.benchmark.complexstreamtrigger.procedures.ComplexTrigger2; 
import edu.brown.benchmark.complexstreamtrigger.procedures.ComplexTrigger3; 
import edu.brown.benchmark.complexstreamtrigger.procedures.ComplexTrigger4; 
import edu.brown.benchmark.complexstreamtrigger.procedures.ComplexTrigger5; 
import edu.brown.benchmark.complexstreamtrigger.procedures.ComplexTrigger6; 
import edu.brown.benchmark.complexstreamtrigger.procedures.ComplexTrigger7; 
import edu.brown.benchmark.complexstreamtrigger.procedures.ComplexTrigger8; 
import edu.brown.benchmark.complexstreamtrigger.procedures.ComplexTrigger9; 
import edu.brown.benchmark.complexstreamtrigger.procedures.ComplexTrigger10; 

public class ComplexStreamTriggerProjectBuilder extends AbstractProjectBuilder {

    // REQUIRED: Retrieved via reflection by BenchmarkController
    public static final Class<? extends BenchmarkComponent> m_clientClass = ComplexStreamTriggerClient.class;

    // REQUIRED: Retrieved via reflection by BenchmarkController
    public static final Class<? extends BenchmarkComponent> m_loaderClass = ComplexStreamTriggerLoader.class;

	// a list of procedures implemented in this benchmark
    @SuppressWarnings("unchecked")
    public static final Class<? extends VoltProcedure> PROCEDURES[] = (Class<? extends VoltProcedure>[])new Class<?>[] {
        ComplexTrigger10.class,
        ComplexTrigger9.class,
        ComplexTrigger8.class,
        ComplexTrigger7.class,
        ComplexTrigger6.class,
        ComplexTrigger5.class,
        ComplexTrigger4.class,
        ComplexTrigger3.class,
        ComplexTrigger2.class,
        ComplexTrigger1.class,
        Initialize.class,
        SimpleCall.class
    };
	
	{
		//addTransactionFrequency(SimpleCall.class, 100);
	}
	
	// a list of tables used in this benchmark with corresponding partitioning keys
    public static final String PARTITIONING[][] = new String[][] {
        { "votes_by_phone_number", "phone_number" }
    };


    public ComplexStreamTriggerProjectBuilder() {
        super("complexstreamtrigger", ComplexStreamTriggerProjectBuilder.class, PROCEDURES, PARTITIONING);
    }
}


package edu.brown.benchmark.streamtrigger;

import org.voltdb.VoltProcedure;

import edu.brown.benchmark.AbstractProjectBuilder;
import edu.brown.api.BenchmarkComponent;

import edu.brown.benchmark.streamtrigger.procedures.SimpleCall; 
import edu.brown.benchmark.streamtrigger.procedures.SimpleTrigger1; 
import edu.brown.benchmark.streamtrigger.procedures.SimpleTrigger2; 
import edu.brown.benchmark.streamtrigger.procedures.SimpleTrigger3; 
import edu.brown.benchmark.streamtrigger.procedures.SimpleTrigger4; 
import edu.brown.benchmark.streamtrigger.procedures.SimpleTrigger5; 
import edu.brown.benchmark.streamtrigger.procedures.SimpleTrigger6; 
import edu.brown.benchmark.streamtrigger.procedures.SimpleTrigger7; 
import edu.brown.benchmark.streamtrigger.procedures.SimpleTrigger8; 
import edu.brown.benchmark.streamtrigger.procedures.SimpleTrigger9; 
import edu.brown.benchmark.streamtrigger.procedures.SimpleTrigger10; 
import edu.brown.benchmark.streamtrigger.procedures.SimpleTrigger11; 
import edu.brown.benchmark.streamtrigger.procedures.SimpleTrigger12; 
import edu.brown.benchmark.streamtrigger.procedures.SimpleTrigger13; 
import edu.brown.benchmark.streamtrigger.procedures.SimpleTrigger14; 
import edu.brown.benchmark.streamtrigger.procedures.SimpleTrigger15; 
import edu.brown.benchmark.streamtrigger.procedures.SimpleTrigger16; 
import edu.brown.benchmark.streamtrigger.procedures.SimpleTrigger17; 
import edu.brown.benchmark.streamtrigger.procedures.SimpleTrigger18; 
import edu.brown.benchmark.streamtrigger.procedures.SimpleTrigger19; 
import edu.brown.benchmark.streamtrigger.procedures.SimpleTrigger20; 
import edu.brown.benchmark.streamtrigger.procedures.SimpleTrigger21; 
import edu.brown.benchmark.streamtrigger.procedures.SimpleTrigger22; 
import edu.brown.benchmark.streamtrigger.procedures.SimpleTrigger23; 
import edu.brown.benchmark.streamtrigger.procedures.SimpleTrigger24; 
import edu.brown.benchmark.streamtrigger.procedures.SimpleTrigger25; 
import edu.brown.benchmark.streamtrigger.procedures.SimpleTrigger26; 
import edu.brown.benchmark.streamtrigger.procedures.SimpleTrigger27; 
import edu.brown.benchmark.streamtrigger.procedures.SimpleTrigger28; 
import edu.brown.benchmark.streamtrigger.procedures.SimpleTrigger29; 
import edu.brown.benchmark.streamtrigger.procedures.SimpleTrigger30; 
import edu.brown.benchmark.streamtrigger.procedures.SimpleTrigger31; 
import edu.brown.benchmark.streamtrigger.procedures.SimpleTrigger32; 
import edu.brown.benchmark.streamtrigger.procedures.SimpleTrigger33; 
import edu.brown.benchmark.streamtrigger.procedures.SimpleTrigger34; 
import edu.brown.benchmark.streamtrigger.procedures.SimpleTrigger35; 
import edu.brown.benchmark.streamtrigger.procedures.SimpleTrigger36; 
import edu.brown.benchmark.streamtrigger.procedures.SimpleTrigger37; 
import edu.brown.benchmark.streamtrigger.procedures.SimpleTrigger38; 
import edu.brown.benchmark.streamtrigger.procedures.SimpleTrigger39; 
import edu.brown.benchmark.streamtrigger.procedures.SimpleTrigger40; 
import edu.brown.benchmark.streamtrigger.procedures.SimpleTrigger41; 
import edu.brown.benchmark.streamtrigger.procedures.SimpleTrigger42; 
import edu.brown.benchmark.streamtrigger.procedures.SimpleTrigger43; 
import edu.brown.benchmark.streamtrigger.procedures.SimpleTrigger44; 
import edu.brown.benchmark.streamtrigger.procedures.SimpleTrigger45; 
import edu.brown.benchmark.streamtrigger.procedures.SimpleTrigger46; 
import edu.brown.benchmark.streamtrigger.procedures.SimpleTrigger47; 
import edu.brown.benchmark.streamtrigger.procedures.SimpleTrigger48; 
import edu.brown.benchmark.streamtrigger.procedures.SimpleTrigger49; 
import edu.brown.benchmark.streamtrigger.procedures.SimpleTrigger50; 
import edu.brown.benchmark.streamtrigger.procedures.TestCall; 

public class StreamTriggerProjectBuilder extends AbstractProjectBuilder {

    // REQUIRED: Retrieved via reflection by BenchmarkController
    public static final Class<? extends BenchmarkComponent> m_clientClass = StreamTriggerClient.class;

    // REQUIRED: Retrieved via reflection by BenchmarkController
    public static final Class<? extends BenchmarkComponent> m_loaderClass = StreamTriggerLoader.class;

	// a list of procedures implemented in this benchmark
    @SuppressWarnings("unchecked")
    public static final Class<? extends VoltProcedure> PROCEDURES[] = (Class<? extends VoltProcedure>[])new Class<?>[] {
        TestCall.class,
        SimpleTrigger50.class,
        SimpleTrigger49.class,
        SimpleTrigger48.class,
        SimpleTrigger47.class,
        SimpleTrigger46.class,
        SimpleTrigger45.class,
        SimpleTrigger44.class,
        SimpleTrigger43.class,
        SimpleTrigger42.class,
        SimpleTrigger41.class,
        SimpleTrigger40.class,
        SimpleTrigger39.class,
        SimpleTrigger38.class,
        SimpleTrigger37.class,
        SimpleTrigger36.class,
        SimpleTrigger35.class,
        SimpleTrigger34.class,
        SimpleTrigger33.class,
        SimpleTrigger32.class,
        SimpleTrigger31.class,
        SimpleTrigger30.class,
        SimpleTrigger29.class,
        SimpleTrigger28.class,
        SimpleTrigger27.class,
        SimpleTrigger26.class,
        SimpleTrigger25.class,
        SimpleTrigger24.class,
        SimpleTrigger23.class,
        SimpleTrigger22.class,
        SimpleTrigger21.class,
        SimpleTrigger20.class,
        SimpleTrigger19.class,
        SimpleTrigger18.class,
        SimpleTrigger17.class,
        SimpleTrigger16.class,
        SimpleTrigger15.class,
        SimpleTrigger14.class,
        SimpleTrigger13.class,
        SimpleTrigger12.class,
        SimpleTrigger11.class,
        SimpleTrigger10.class,
        SimpleTrigger9.class,
        SimpleTrigger8.class,
        SimpleTrigger7.class,
        SimpleTrigger6.class,
        SimpleTrigger5.class,
        SimpleTrigger4.class,
        SimpleTrigger3.class,
        SimpleTrigger2.class,
        SimpleTrigger1.class,
        SimpleCall.class
    };
	
	{
		addTransactionFrequency(SimpleCall.class, 100);
	}
	
	// a list of tables used in this benchmark with corresponding partitioning keys
    public static final String PARTITIONING[][] = new String[][] {
    };

    public StreamTriggerProjectBuilder() {
        super("streamtrigger", StreamTriggerProjectBuilder.class, PROCEDURES, PARTITIONING);
    }
}


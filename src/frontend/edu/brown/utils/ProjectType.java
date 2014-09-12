/***************************************************************************
 *  Copyright (C) 2012 by H-Store Project                                  *
 *  Brown University                                                       *
 *  Massachusetts Institute of Technology                                  *
 *  Yale University                                                        *
 *                                                                         *
 *  http://hstore.cs.brown.edu/                                            *
 *                                                                         *
 *  Permission is hereby granted, free of charge, to any person obtaining  *
 *  a copy of this software and associated documentation files (the        *
 *  "Software"), to deal in the Software without restriction, including    *
 *  without limitation the rights to use, copy, modify, merge, publish,    *
 *  distribute, sublicense, and/or sell copies of the Software, and to     *
 *  permit persons to whom the Software is furnished to do so, subject to  *
 *  the following conditions:                                              *
 *                                                                         *
 *  The above copyright notice and this permission notice shall be         *
 *  included in all copies or substantial portions of the Software.        *
 *                                                                         *
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,        *
 *  EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF     *
 *  MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. *
 *  IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR      *
 *  OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,  *
 *  ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR  *
 *  OTHER DEALINGS IN THE SOFTWARE.                                        *
 ***************************************************************************/
package edu.brown.utils;

import java.io.File;
import java.io.IOException;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

public enum ProjectType {

    TPCC("TPC-C", "org.voltdb.benchmark.tpcc"),
    TPCE("TPC-E", "edu.brown.benchmark.tpce"),
    TPCEB("TPC-E", "edu.brown.benchmark.tpceb"),
    TM1("TM1", "edu.brown.benchmark.tm1"),
    SIMPLE("Simple", "edu.brown.benchmark.simple"),
    SEATS("SEATS", "edu.brown.benchmark.seats"),
    MARKOV("Markov", "edu.brown.benchmark.markov"),
    BINGO("Bingo", "org.voltdb.benchmark.bingo"),
    AUCTIONMARK("AuctionMark", "edu.brown.benchmark.auctionmark"),
    LOCALITY("Locality", "edu.brown.benchmark.locality"),
    MAPREDUCE("MapReduce", "edu.brown.benchmark.mapreduce"),
    WIKIPEDIA("Wikipedia", "edu.brown.benchmark.wikipedia"),
    BIKER("BIKER", "edu.brown.benchmark.biker"),
    YCSB("YCSB", "edu.brown.benchmark.ycsb"), 
    VOTER("Voter", "edu.brown.benchmark.voter"),
    SMALLBANK("SmallBank", "edu.brown.benchmark.smallbank"),
    SIMPLEWINDOW("SimpleWindow", "edu.brown.benchmark.simplewindow"),
    EXAMPLE("Example", "edu.brown.benchmark.example"),
    UPSERT("Upsert", "edu.brown.benchmark.upsert"),
    STREAMEXAMPLE("StreamExample", "edu.brown.benchmark.streamexample"),
    VOTERSTREAM("VoterStream", "edu.brown.benchmark.voterstream"),
    BIKERSTREAM("BikerStream", "edu.brown.benchmark.bikerstream"),
    ANOTHERSTREAM("AnotherStream","edu.brown.benchmark.anotherstream"),
    ANOTHERVOTER("AnotherVoter","edu.brown.benchmark.anothervoter"),
    YETANOTHERVOTER1("YetAnotherVoter1","edu.brown.benchmark.yetanothervoter1"),
    YETANOTHERVOTER2("YetAnotherVoter2","edu.brown.benchmark.yetanothervoter2"),
    YETANOTHERVOTER3("YetAnotherVoter3","edu.brown.benchmark.yetanothervoter3"),
    YETANOTHERVOTER4("YetAnotherVoter4","edu.brown.benchmark.yetanothervoter4"),
    YETANOTHERVOTER5("YetAnotherVoter5","edu.brown.benchmark.yetanothervoter5"),
    STREAMTRIGGER("StreamTrigger","edu.brown.benchmark.streamtrigger"),
    COMPLEXSTREAMTRIGGER("ComplexStreamTrigger","edu.brown.benchmark.complexstreamtrigger"),
    NOSTREAMTRIGGER("NoStreamTrigger","edu.brown.benchmark.nostreamtrigger"),
    COMPLEXNOSTREAMTRIGGER("ComplexNoStreamTrigger","edu.brown.benchmark.complexnostreamtrigger"),
    NOSTREAMTRIGGER10("NoStreamTrigger10","edu.brown.benchmark.nostreamtrigger10"),
    NOSTREAMTRIGGER20("NoStreamTrigger20","edu.brown.benchmark.nostreamtrigger20"),
    NOSTREAMTRIGGER30("NoStreamTrigger30","edu.brown.benchmark.nostreamtrigger30"),
    NOSTREAMTRIGGER40("NoStreamTrigger40","edu.brown.benchmark.nostreamtrigger40"),
    RECOVERY("Recovery","edu.brown.benchmark.recovery"),
    NOSTREAMTRIGGER1("NoStreamTrigger1","edu.brown.benchmark.nostreamtrigger1"),
    NOSTREAMTRIGGER2("NoStreamTrigger2","edu.brown.benchmark.nostreamtrigger2"),
    NOSTREAMTRIGGER3("NoStreamTrigger3","edu.brown.benchmark.nostreamtrigger3"),
    NOSTREAMTRIGGER4("NoStreamTrigger4","edu.brown.benchmark.nostreamtrigger4"),
    NOSTREAMTRIGGER5("NoStreamTrigger5","edu.brown.benchmark.nostreamtrigger5"),
    FRONTENDTRIGGER("FrontEndTrigger","edu.brown.benchmark.frontendtrigger"),
    SIMPLEFRONTENDTRIGGER("SimpleFrontEndTrigger","edu.brown.benchmark.simplefrontendtrigger"),
    ANOTHERVOTERSTREAM("AnotherVoterStream","edu.brown.benchmark.anothervoterstream"),
    VOTERSTREAMWINDOWS("VoterStreamWindows","edu.brown.benchmark.voterstreamwindows"),
    ANOTHERVOTERWINDOWS("AnotherVoterWindows","edu.brown.benchmark.anothervoterwindows"),
    SIMPLEWINDOWHSTORE("SimpleWindowHStore", "edu.brown.benchmark.simplewindowhstore"),
    SIMPLEWINDOWSSTORE("SimpleWindowSStore", "edu.brown.benchmark.simplewindowsstore"),
    MICROWINHSTOREFULL("MicroWinHStoreFull", "edu.brown.benchmark.microwinhstorefull"),
    MICROWINHSTORECLEANUP("MicroWinHStoreCleanup", "edu.brown.benchmark.microwinhstorecleanup"),
    MICROWINHSTORENOCLEANUP("MicroWinHStoreNoCleanup", "edu.brown.benchmark.microwinhstorenocleanup"),
    MICROWINHSTORESOMECLEANUP("MicroWinHStoreSomeCleanup", "edu.brown.benchmark.microwinhstoresomecleanup"),
    MICROWINSSTORE("MicroWinSStore", "edu.brown.benchmark.microwinsstore"),
    MICROWINTIMEHSTOREFULL("MicroWinTimeHStoreFull", "edu.brown.benchmark.microwintimehstorefull"),
    MICROWINTIMEHSTORECLEANUP("MicroWinTimeHStoreCleanup", "edu.brown.benchmark.microwintimehstorecleanup"),
    MICROWINTIMEHSTORENOCLEANUP("MicroWinTimeHStoreNoCleanup", "edu.brown.benchmark.microwintimehstorenocleanup"),
    MICROWINTIMEHSTORESOMECLEANUP("MicroWinTimeHStoreSomeCleanup", "edu.brown.benchmark.microwintimehstoresomecleanup"),
    MICROWINTIMESSTORE("MicroWinTimeSStore", "edu.brown.benchmark.microwintimesstore"),
    VOTERWINHSTORE("VoterWinHStore", "edu.brown.benchmark.voterwinhstore"),
    VOTERWINHSTORENOCLEANUP("VoterWinHStoreNoCleanup", "edu.brown.benchmark.voterwinhstorenocleanup"),
    VOTERWINSSTORE("VoterWinSStore", "edu.brown.benchmark.voterwinsstore"),
    VOTERWINTIMEHSTORE("VoterWinTimeHStore", "edu.brown.benchmark.voterwintimehstore"),
    VOTERWINTIMEHSTORENOCLEANUP("VoterWinTimeHStoreNoCleanup", "edu.brown.benchmark.voterwintimehstorenocleanup"),
    VOTERWINTIMEHSTORENOCLEANUPANOTHER("VoterWinTimeHStoreNoCleanupAnother", "edu.brown.benchmark.voterwintimehstorenocleanupanother"),
    VOTERWINTIMEHSTOREANOTHER("VoterWinTimeHStoreAnother", "edu.brown.benchmark.voterwintimehstoreanother"),
    VOTERHSTOREEETRIGTEST("VoterHStoreEETrigTest", "edu.brown.benchmark.voterhstoreeetrigtest"),
    VOTERWINTIMESSTORE("VoterWinTimeSStore", "edu.brown.benchmark.voterwintimesstore"),
    VOTERWINTIMESSTOREFULLSTREAM("VoterWinTimeSStoreFullStream", "edu.brown.benchmark.voterwintimesstorefullstream"),
    VOTERWINTIMESSTOREWINONLY("VoterWinTimeSStoreWinOnly", "edu.brown.benchmark.voterwintimesstorewinonly"),
    VOTERSSTOREEETRIGTEST("VoterSStoreEETrigTest", "edu.brown.benchmark.votersstoreeetrigtest"),
    SIMPLESTREAMTRIGGER("SimpleStreamTrigger", "edu.brown.benchmark.simplestreamtrigger"),
    SIMPLENOOP("SimpleNoOp", "edu.brown.benchmark.simplenoop"),
    SIMPLESTATEMENT("SimpleStatement", "edu.brown.benchmark.simplestatement"),
    SIMPLEDISTRIBUTION("SimpleDistribution", "edu.brown.benchmark.simpledistribution"),
    TEST("Test", null),
    WORDCOUNT("WordCount", "edu.brown.benchmark.wordcount"),
    WORDCOUNTHSTORE("WordCountHStore", "edu.brown.benchmark.wordcounthstore"),
    WORDCOUNTSSTORE("WordCountSStore", "edu.brown.benchmark.wordcountsstore"),
    WORDCOUNTSSTOREGETRESULTS("WordCountSStoreGetResults", "edu.brown.benchmark.wordcountsstoregetresults"),
    WORDCOUNTSSTOREWITHBATCH("WordCountSStoreWithBatch", "edu.brown.benchmark.wordcountsstorewithbatch"),
    VOTERDEMOHSTORE("VoterDemoHStore", "edu.brown.benchmark.voterdemohstore"),
    VOTERDEMOHSTOREWINSP1("VoterDemoHStoreWinSP1", "edu.brown.benchmark.voterdemohstorewinsp1"),
    VOTERDEMOHSTORENOCLEANUP("VoterDemoHStoreNoCleanup", "edu.brown.benchmark.voterdemohstorenocleanup"),
    VOTERDEMOHSTORENOCLEANUPANOTHER("VoterDemoHStoreNoCleanupAnother", "edu.brown.benchmark.voterdemohstorenocleanupanother"),
    VOTERDEMOHSTOREANOTHER("VoterDemoHStoreAnother", "edu.brown.benchmark.voterdemohstoreanother"),
    VOTERDEMOSSTORE("VoterDemoSStore", "edu.brown.benchmark.voterdemosstore"),
    VOTERDEMOSSTOREWINSP1("VoterDemoSStoreWinSP1", "edu.brown.benchmark.voterdemosstorewinsp1"),
    VOTERDEMOSSTOREWINSP1SCHEDULER("VoterDemoSStoreWinSP1Scheduler", "edu.brown.benchmark.voterdemosstorewinsp1scheduler"),
    VOTERDEMOSSTOREANOTHER("VoterDemoSStoreAnother", "edu.brown.benchmark.voterdemosstoreanother"),
    VOTERDEMOSSTOREPETRIGONLY("VoterDemoSStorePETrigOnly", "edu.brown.benchmark.voterdemosstorepetrigonly"),
    VOTERDEMOSSTORENOPETRIG("VoterDemoSStoreNoPETrig", "edu.brown.benchmark.voterdemosstorenopetrig"),
    VOTERDEMOSSTORENOPETRIGWINSP1("VoterDemoSStoreNoPETrigWinSP1", "edu.brown.benchmark.voterdemosstorenopetrigwinsp1"),
    
    
    
    VOTERWINHSTOREW100S1("VoterWinHStoreW100S1", "edu.brown.benchmark.voterexperiments.winhstore.w100s1"),
    VOTERWINHSTOREW100S5("VoterWinHStoreW100S5", "edu.brown.benchmark.voterexperiments.winhstore.w100s5"),
    VOTERWINHSTOREW100S10("VoterWinHStoreW100S10", "edu.brown.benchmark.voterexperiments.winhstore.w100s10"),
    VOTERWINHSTOREW100S100("VoterWinHStoreW100S100", "edu.brown.benchmark.voterexperiments.winhstore.w100s100"),
    VOTERWINHSTOREW1000S1("VoterWinHStoreW1000S1", "edu.brown.benchmark.voterexperiments.winhstore.w1000s1"),
    VOTERWINHSTOREW1000S5("VoterWinHStoreW1000S5", "edu.brown.benchmark.voterexperiments.winhstore.w1000s5"),
    VOTERWINHSTOREW1000S10("VoterWinHStoreW1000S10", "edu.brown.benchmark.voterexperiments.winhstore.w1000s10"),
    VOTERWINHSTOREW1000S100("VoterWinHStoreW1000S100", "edu.brown.benchmark.voterexperiments.winhstore.w1000s100"),
    VOTERWINHSTOREW10000S1("VoterWinHStoreW10000S1", "edu.brown.benchmark.voterexperiments.winhstore.w10000s1"),
    VOTERWINHSTOREW10000S5("VoterWinHStoreW10000S5", "edu.brown.benchmark.voterexperiments.winhstore.w10000s5"),
    VOTERWINHSTOREW10000S10("VoterWinHStoreW10000S10", "edu.brown.benchmark.voterexperiments.winhstore.w10000s10"),
    VOTERWINHSTOREW10000S100("VoterWinHStoreW10000S100", "edu.brown.benchmark.voterexperiments.winhstore.w10000s100"),
      
    VOTERWINHSTORENOCLEANUPW100S1("VoterWinHStoreNoCleanupW100S1", "edu.brown.benchmark.voterexperiments.winhstorenocleanup.w100s1"),
    VOTERWINHSTORENOCLEANUPW100S5("VoterWinHStoreNoCleanupW100S5", "edu.brown.benchmark.voterexperiments.winhstorenocleanup.w100s5"),
    VOTERWINHSTORENOCLEANUPW100S10("VoterWinHStoreNoCleanupW100S10", "edu.brown.benchmark.voterexperiments.winhstorenocleanup.w100s10"),
    VOTERWINHSTORENOCLEANUPW100S100("VoterWinHStoreNoCleanupW100S100", "edu.brown.benchmark.voterexperiments.winhstorenocleanup.w100s100"),
    VOTERWINHSTORENOCLEANUPW1000S1("VoterWinHStoreNoCleanupW1000S1", "edu.brown.benchmark.voterexperiments.winhstorenocleanup.w1000s1"),
    VOTERWINHSTORENOCLEANUPW1000S5("VoterWinHStoreNoCleanupW1000S5", "edu.brown.benchmark.voterexperiments.winhstorenocleanup.w1000s5"),
    VOTERWINHSTORENOCLEANUPW1000S10("VoterWinHStoreNoCleanupW1000S10", "edu.brown.benchmark.voterexperiments.winhstorenocleanup.w1000s10"),
    VOTERWINHSTORENOCLEANUPW1000S100("VoterWinHStoreNoCleanupW1000S100", "edu.brown.benchmark.voterexperiments.winhstorenocleanup.w1000s100"),
    VOTERWINHSTORENOCLEANUPW10000S1("VoterWinHStoreNoCleanupW10000S1", "edu.brown.benchmark.voterexperiments.winhstorenocleanup.w10000s1"),
    VOTERWINHSTORENOCLEANUPW10000S5("VoterWinHStoreNoCleanupW10000S5", "edu.brown.benchmark.voterexperiments.winhstorenocleanup.w10000s5"),
    VOTERWINHSTORENOCLEANUPW10000S10("VoterWinHStoreNoCleanupW10000S10", "edu.brown.benchmark.voterexperiments.winhstorenocleanup.w10000s10"),
    VOTERWINHSTORENOCLEANUPW10000S100("VoterWinHStoreNoCleanupW10000S100", "edu.brown.benchmark.voterexperiments.winhstorenocleanup.w10000s100"),

    VOTERWINHSTORENOSTATEW100S1("VoterWinHStoreNoStateW100S1", "edu.brown.benchmark.voterexperiments.winhstorenostate.w100s1"),
    VOTERWINHSTORENOSTATEW100S5("VoterWinHStoreNoStateW100S5", "edu.brown.benchmark.voterexperiments.winhstorenostate.w100s5"),
    VOTERWINHSTORENOSTATEW100S10("VoterWinHStoreNoStateW100S10", "edu.brown.benchmark.voterexperiments.winhstorenostate.w100s10"),
    VOTERWINHSTORENOSTATEW100S100("VoterWinHStoreNoStateW100S100", "edu.brown.benchmark.voterexperiments.winhstorenostate.w100s100"),
    VOTERWINHSTORENOSTATEW1000S1("VoterWinHStoreNoStateW1000S1", "edu.brown.benchmark.voterexperiments.winhstorenostate.w1000s1"),
    VOTERWINHSTORENOSTATEW1000S5("VoterWinHStoreNoStateW1000S5", "edu.brown.benchmark.voterexperiments.winhstorenostate.w1000s5"),
    VOTERWINHSTORENOSTATEW1000S10("VoterWinHStoreNoStateW1000S10", "edu.brown.benchmark.voterexperiments.winhstorenostate.w1000s10"),
    VOTERWINHSTORENOSTATEW1000S100("VoterWinHStoreNoStateW1000S100", "edu.brown.benchmark.voterexperiments.winhstorenostate.w1000s100"),
    VOTERWINHSTORENOSTATEW10000S1("VoterWinHStoreNoStateW10000S1", "edu.brown.benchmark.voterexperiments.winhstorenostate.w10000s1"),
    VOTERWINHSTORENOSTATEW10000S5("VoterWinHStoreNoStateW10000S5", "edu.brown.benchmark.voterexperiments.winhstorenostate.w10000s5"),
    VOTERWINHSTORENOSTATEW10000S10("VoterWinHStoreNoStateW10000S10", "edu.brown.benchmark.voterexperiments.winhstorenostate.w10000s10"),
    VOTERWINHSTORENOSTATEW10000S100("VoterWinHStoreNoStateW10000S100", "edu.brown.benchmark.voterexperiments.winhstorenostate.w10000s100"),
    
    VOTERWINSSTOREW100S1("VoterWinSStoreW100S1", "edu.brown.benchmark.voterexperiments.winsstore.w100s1"),
    VOTERWINSSTOREW100S5("VoterWinSStoreW100S5", "edu.brown.benchmark.voterexperiments.winsstore.w100s5"),
    VOTERWINSSTOREW100S10("VoterWinSStoreW100S10", "edu.brown.benchmark.voterexperiments.winsstore.w100s10"),
    VOTERWINSSTOREW100S10WITHBATCH("VoterWinSStoreW100S10WithBatch", "edu.brown.benchmark.voterexperiments.winsstore.w100s10withbatch"),
    VOTERWINSSTOREW100S100("VoterWinSStoreW100S100", "edu.brown.benchmark.voterexperiments.winsstore.w100s100"),
    VOTERWINSSTOREW1000S1("VoterWinSStoreW1000S1", "edu.brown.benchmark.voterexperiments.winsstore.w1000s1"),
    VOTERWINSSTOREW1000S5("VoterWinSStoreW1000S5", "edu.brown.benchmark.voterexperiments.winsstore.w1000s5"),
    VOTERWINSSTOREW1000S10("VoterWinSStoreW1000S10", "edu.brown.benchmark.voterexperiments.winsstore.w1000s10"),
    VOTERWINSSTOREW1000S100("VoterWinSStoreW1000S100", "edu.brown.benchmark.voterexperiments.winsstore.w1000s100"),
    VOTERWINSSTOREW10000S1("VoterWinSStoreW10000S1", "edu.brown.benchmark.voterexperiments.winsstore.w10000s1"),
    VOTERWINSSTOREW10000S5("VoterWinSStoreW10000S5", "edu.brown.benchmark.voterexperiments.winsstore.w10000s5"),
    VOTERWINSSTOREW10000S10("VoterWinSStoreW10000S10", "edu.brown.benchmark.voterexperiments.winsstore.w10000s10"),
    VOTERWINSSTOREW10000S100("VoterWinSStoreW10000S100", "edu.brown.benchmark.voterexperiments.winsstore.w10000s100"),
    
    VOTERDEMOHSTOREWXSYY("VoterDemoHStoreWXSYY", "edu.brown.benchmark.voterexperiments.demohstore.wXsYY"),
    VOTERDEMOSSTOREWXSYY("VoterDemoSStoreWXSYY", "edu.brown.benchmark.voterexperiments.demosstore.wXsYY"),
    
    VOTERDEMOHSTORECORRECT("VoterDemoHStoreCorrect", "edu.brown.benchmark.voterexperiments.demohstorecorrect"),
    VOTERDEMOSSTORECORRECT("VoterDemoSStoreCorrect", "edu.brown.benchmark.voterexperiments.demosstorecorrect"),
    
    MICROEXPFTRIGGERS("MicroExpFTriggers", "edu.brown.benchmark.microexperiments.ftriggers.orig"),
    MICROEXPFTRIGGERSTRIG1("MicroExpFTriggersTrig1", "edu.brown.benchmark.microexperiments.ftriggers.trig1"),
    MICROEXPFTRIGGERSTRIG2("MicroExpFTriggersTrig2", "edu.brown.benchmark.microexperiments.ftriggers.trig2"),
    MICROEXPFTRIGGERSTRIG3("MicroExpFTriggersTrig3", "edu.brown.benchmark.microexperiments.ftriggers.trig3"),
    MICROEXPFTRIGGERSTRIG4("MicroExpFTriggersTrig4", "edu.brown.benchmark.microexperiments.ftriggers.trig4"),
    MICROEXPFTRIGGERSTRIG5("MicroExpFTriggersTrig5", "edu.brown.benchmark.microexperiments.ftriggers.trig5"),
    MICROEXPFTRIGGERSTRIG6("MicroExpFTriggersTrig6", "edu.brown.benchmark.microexperiments.ftriggers.trig6"),
    MICROEXPFTRIGGERSTRIG7("MicroExpFTriggersTrig7", "edu.brown.benchmark.microexperiments.ftriggers.trig7"),
    MICROEXPFTRIGGERSTRIG8("MicroExpFTriggersTrig8", "edu.brown.benchmark.microexperiments.ftriggers.trig8"),
    MICROEXPFTRIGGERSTRIG9("MicroExpFTriggersTrig9", "edu.brown.benchmark.microexperiments.ftriggers.trig9"),
    MICROEXPFTRIGGERSTRIG10("MicroExpFTriggersTrig10", "edu.brown.benchmark.microexperiments.ftriggers.trig10"),

    MICROEXPNOFTRIGGERS("MicroExpNoFTriggers", "edu.brown.benchmark.microexperiments.noftriggers.orig"),
    MICROEXPNOFTRIGGERSTRIG1("MicroExpNoFTriggersTrig1", "edu.brown.benchmark.microexperiments.noftriggers.trig1"),
    MICROEXPNOFTRIGGERSTRIG2("MicroExpNoFTriggersTrig2", "edu.brown.benchmark.microexperiments.noftriggers.trig2"),
    MICROEXPNOFTRIGGERSTRIG3("MicroExpNoFTriggersTrig3", "edu.brown.benchmark.microexperiments.noftriggers.trig3"),
    MICROEXPNOFTRIGGERSTRIG4("MicroExpNoFTriggersTrig4", "edu.brown.benchmark.microexperiments.noftriggers.trig4"),
    MICROEXPNOFTRIGGERSTRIG5("MicroExpNoFTriggersTrig5", "edu.brown.benchmark.microexperiments.noftriggers.trig5"),
    MICROEXPNOFTRIGGERSTRIG6("MicroExpNoFTriggersTrig6", "edu.brown.benchmark.microexperiments.noftriggers.trig6"),
    MICROEXPNOFTRIGGERSTRIG7("MicroExpNoFTriggersTrig7", "edu.brown.benchmark.microexperiments.noftriggers.trig7"),
    MICROEXPNOFTRIGGERSTRIG8("MicroExpNoFTriggersTrig8", "edu.brown.benchmark.microexperiments.noftriggers.trig8"),
    MICROEXPNOFTRIGGERSTRIG9("MicroExpNoFTriggersTrig9", "edu.brown.benchmark.microexperiments.noftriggers.trig9"),
    MICROEXPNOFTRIGGERSTRIG10("MicroExpNoFTriggersTrig10", "edu.brown.benchmark.microexperiments.noftriggers.trig10"),
    
    MICROEXPBTRIGGERS("MicroExpBTriggers", "edu.brown.benchmark.microexperiments.btriggers.orig"),
    MICROEXPBTRIGGERSTRIG1("MicroExpBTriggersTrig1", "edu.brown.benchmark.microexperiments.btriggers.trig1"),
    MICROEXPBTRIGGERSTRIG2("MicroExpBTriggersTrig2", "edu.brown.benchmark.microexperiments.btriggers.trig2"),
    MICROEXPBTRIGGERSTRIG3("MicroExpBTriggersTrig3", "edu.brown.benchmark.microexperiments.btriggers.trig3"),
    MICROEXPBTRIGGERSTRIG4("MicroExpBTriggersTrig4", "edu.brown.benchmark.microexperiments.btriggers.trig4"),
    MICROEXPBTRIGGERSTRIG5("MicroExpBTriggersTrig5", "edu.brown.benchmark.microexperiments.btriggers.trig5"),
    MICROEXPBTRIGGERSTRIG6("MicroExpBTriggersTrig6", "edu.brown.benchmark.microexperiments.btriggers.trig6"),
    MICROEXPBTRIGGERSTRIG7("MicroExpBTriggersTrig7", "edu.brown.benchmark.microexperiments.btriggers.trig7"),
    MICROEXPBTRIGGERSTRIG8("MicroExpBTriggersTrig8", "edu.brown.benchmark.microexperiments.btriggers.trig8"),
    MICROEXPBTRIGGERSTRIG9("MicroExpBTriggersTrig9", "edu.brown.benchmark.microexperiments.btriggers.trig9"),
    MICROEXPBTRIGGERSTRIG10("MicroExpBTriggersTrig10", "edu.brown.benchmark.microexperiments.btriggers.trig10"),

    MICROEXPNOBTRIGGERS("MicroExpNoBTriggers", "edu.brown.benchmark.microexperiments.nobtriggers.orig"),
    MICROEXPNOBTRIGGERSTRIG1("MicroExpNoBTriggersTrig1", "edu.brown.benchmark.microexperiments.nobtriggers.trig1"),
    MICROEXPNOBTRIGGERSTRIG2("MicroExpNoBTriggersTrig2", "edu.brown.benchmark.microexperiments.nobtriggers.trig2"),
    MICROEXPNOBTRIGGERSTRIG3("MicroExpNoBTriggersTrig3", "edu.brown.benchmark.microexperiments.nobtriggers.trig3"),
    MICROEXPNOBTRIGGERSTRIG4("MicroExpNoBTriggersTrig4", "edu.brown.benchmark.microexperiments.nobtriggers.trig4"),
    MICROEXPNOBTRIGGERSTRIG5("MicroExpNoBTriggersTrig5", "edu.brown.benchmark.microexperiments.nobtriggers.trig5"),
    MICROEXPNOBTRIGGERSTRIG6("MicroExpNoBTriggersTrig6", "edu.brown.benchmark.microexperiments.nobtriggers.trig6"),
    MICROEXPNOBTRIGGERSTRIG7("MicroExpNoBTriggersTrig7", "edu.brown.benchmark.microexperiments.nobtriggers.trig7"),
    MICROEXPNOBTRIGGERSTRIG8("MicroExpNoBTriggersTrig8", "edu.brown.benchmark.microexperiments.nobtriggers.trig8"),
    MICROEXPNOBTRIGGERSTRIG9("MicroExpNoBTriggersTrig9", "edu.brown.benchmark.microexperiments.nobtriggers.trig9"),
    MICROEXPNOBTRIGGERSTRIG10("MicroExpNoBTriggersTrig10", "edu.brown.benchmark.microexperiments.nobtriggers.trig10"),
    
    MICROEXPWINDOWS("MicroExpWindows", "edu.brown.benchmark.microexperiments.windows.orig"),
    MICROEXPNOWINDOWS("MicroExpNoWindows", "edu.brown.benchmark.microexperiments.nowindows.orig"),
    
    MICROEXPWINDOWSW100S1("MicroExpWindowsW100S1", "edu.brown.benchmark.microexp.windows.w100s1"),
    MICROEXPWINDOWSW100S5("MicroExpWindowsW100S5", "edu.brown.benchmark.microexp.windows.w100s5"),
    MICROEXPWINDOWSW100S10("MicroExpWindowsW100S10", "edu.brown.benchmark.microexp.windows.w100s10"),
    MICROEXPWINDOWSW100S30("MicroExpWindowsW100S30", "edu.brown.benchmark.microexp.windows.w100s30"),
    MICROEXPWINDOWSW100S100("MicroExpWindowsW100S100", "edu.brown.benchmark.microexp.windows.w100s100"),
    MICROEXPWINDOWSW10S2("MicroExpWindowsW10S2", "edu.brown.benchmark.microexp.windows.w10s2"),
    MICROEXPWINDOWSW100S2("MicroExpWindowsW100S2", "edu.brown.benchmark.microexp.windows.w100s2"),
    MICROEXPWINDOWSW1000S2("MicroExpWindowsW1000S2", "edu.brown.benchmark.microexp.windows.w1000s2"),
    MICROEXPWINDOWSW10000S2("MicroExpWindowsW10000S2", "edu.brown.benchmark.microexp.windows.w10000s2"),
    MICROEXPWINDOWSW100000S2("MicroExpWindowsW100000S2", "edu.brown.benchmark.microexp.windows.w100000s2"),
    
    MICROEXPNOWINDOWSW100S1("MicroExpNoWindowsW100S1", "edu.brown.benchmark.microexp.nowindows.w100s1"),
    MICROEXPNOWINDOWSW100S5("MicroExpNoWindowsW100S5", "edu.brown.benchmark.microexp.nowindows.w100s5"),
    MICROEXPNOWINDOWSW100S10("MicroExpNoWindowsW100S10", "edu.brown.benchmark.microexp.nowindows.w100s10"),
    MICROEXPNOWINDOWSW100S30("MicroExpNoWindowsW100S30", "edu.brown.benchmark.microexp.nowindows.w100s30"),
    MICROEXPNOWINDOWSW100S100("MicroExpNoWindowsW100S100", "edu.brown.benchmark.microexp.nowindows.w100s100"),
    MICROEXPNOWINDOWSW10S2("MicroExpNoWindowsW10S2", "edu.brown.benchmark.microexp.nowindows.w10s2"),
    MICROEXPNOWINDOWSW100S2("MicroExpNoWindowsW100S2", "edu.brown.benchmark.microexp.nowindows.w100s2"),
    MICROEXPNOWINDOWSW1000S2("MicroExpNoWindowsW1000S2", "edu.brown.benchmark.microexp.nowindows.w1000s2"),
    MICROEXPNOWINDOWSW10000S2("MicroExpNoWindowsW10000S2", "edu.brown.benchmark.microexp.nowindows.w10000s2"),
    MICROEXPNOWINDOWSW100000S2("MicroExpNoWindowsW100000S2", "edu.brown.benchmark.microexp.nowindows.w100000s2"),

    MICROEXPROUTETRIG("MicroExpRouteTrigTrig", "edu.brown.benchmark.microexperiments.routetrig.orig"),
    MICROEXPROUTETRIGTRIG1("MicroExpRouteTrigTrig1", "edu.brown.benchmark.microexperiments.routetrig.trig1"),
    MICROEXPROUTETRIGTRIG2("MicroExpRouteTrigTrig2", "edu.brown.benchmark.microexperiments.routetrig.trig2"),
    MICROEXPROUTETRIGTRIG3("MicroExpRouteTrigTrig3", "edu.brown.benchmark.microexperiments.routetrig.trig3"),
    MICROEXPROUTETRIGTRIG4("MicroExpRouteTrigTrig4", "edu.brown.benchmark.microexperiments.routetrig.trig4"),
    MICROEXPROUTETRIGTRIG5("MicroExpRouteTrigTrig5", "edu.brown.benchmark.microexperiments.routetrig.trig5"),
    MICROEXPROUTETRIGTRIG6("MicroExpRouteTrigTrig6", "edu.brown.benchmark.microexperiments.routetrig.trig6"),
    MICROEXPROUTETRIGTRIG7("MicroExpRouteTrigTrig7", "edu.brown.benchmark.microexperiments.routetrig.trig7"),
    MICROEXPROUTETRIGTRIG8("MicroExpRouteTrigTrig8", "edu.brown.benchmark.microexperiments.routetrig.trig8"),
    MICROEXPROUTETRIGTRIG9("MicroExpRouteTrigTrig9", "edu.brown.benchmark.microexperiments.routetrig.trig9"),
    MICROEXPROUTETRIGTRIG10("MicroExpRouteTrigTrig10", "edu.brown.benchmark.microexperiments.routetrig.trig10"),

    MICROEXPNOROUTETRIGTRIG("MicroExpNoRouteTrigTrig", "edu.brown.benchmark.microexperiments.noroutetrig.orig"),
    MICROEXPNOROUTETRIGTRIG1("MicroExpNoRouteTrigTrig1", "edu.brown.benchmark.microexperiments.noroutetrig.trig1"),
    MICROEXPNOROUTETRIGTRIG2("MicroExpNoRouteTrigTrig2", "edu.brown.benchmark.microexperiments.noroutetrig.trig2"),
    MICROEXPNOROUTETRIGTRIG3("MicroExpNoRouteTrigTrig3", "edu.brown.benchmark.microexperiments.noroutetrig.trig3"),
    MICROEXPNOROUTETRIGTRIG4("MicroExpNoRouteTrigTrig4", "edu.brown.benchmark.microexperiments.noroutetrig.trig4"),
    MICROEXPNOROUTETRIGTRIG5("MicroExpNoRouteTrigTrig5", "edu.brown.benchmark.microexperiments.noroutetrig.trig5"),
    MICROEXPNOROUTETRIGTRIG6("MicroExpNoRouteTrigTrig6", "edu.brown.benchmark.microexperiments.noroutetrig.trig6"),
    MICROEXPNOROUTETRIGTRIG7("MicroExpNoRouteTrigTrig7", "edu.brown.benchmark.microexperiments.noroutetrig.trig7"),
    MICROEXPNOROUTETRIGTRIG8("MicroExpNoRouteTrigTrig8", "edu.brown.benchmark.microexperiments.noroutetrig.trig8"),
    MICROEXPNOROUTETRIGTRIG9("MicroExpNoRouteTrigTrig9", "edu.brown.benchmark.microexperiments.noroutetrig.trig9"),
    MICROEXPNOROUTETRIGTRIG10("MicroExpNoRouteTrigTrig10", "edu.brown.benchmark.microexperiments.noroutetrig.trig10"),
    
    MICROEXPROUTETRIGCLIENTTRIG("MicroExpRouteTrigClient", "edu.brown.benchmark.microexperiments.routetrigclient.orig"),
    MICROEXPROUTETRIGCLIENTTRIG1("MicroExpRouteTrigClientTrig1", "edu.brown.benchmark.microexperiments.routetrigclient.trig1"),
    MICROEXPROUTETRIGCLIENTTRIG2("MicroExpRouteTrigClientTrig2", "edu.brown.benchmark.microexperiments.routetrigclient.trig2"),
    MICROEXPROUTETRIGCLIENTTRIG3("MicroExpRouteTrigClientTrig3", "edu.brown.benchmark.microexperiments.routetrigclient.trig3"),
    MICROEXPROUTETRIGCLIENTTRIG4("MicroExpRouteTrigClientTrig4", "edu.brown.benchmark.microexperiments.routetrigclient.trig4"),
    MICROEXPROUTETRIGCLIENTTRIG5("MicroExpRouteTrigClientTrig5", "edu.brown.benchmark.microexperiments.routetrigclient.trig5"),
    MICROEXPROUTETRIGCLIENTTRIG6("MicroExpRouteTrigClientTrig6", "edu.brown.benchmark.microexperiments.routetrigclient.trig6"),
    MICROEXPROUTETRIGCLIENTTRIG7("MicroExpRouteTrigClientTrig7", "edu.brown.benchmark.microexperiments.routetrigclient.trig7"),
    MICROEXPROUTETRIGCLIENTTRIG8("MicroExpRouteTrigClientTrig8", "edu.brown.benchmark.microexperiments.routetrigclient.trig8"),
    MICROEXPROUTETRIGCLIENTTRIG9("MicroExpRouteTrigClientTrig9", "edu.brown.benchmark.microexperiments.routetrigclient.trig9"),
    MICROEXPROUTETRIGCLIENTTRIG10("MicroExpRouteTrigClientTrig10", "edu.brown.benchmark.microexperiments.routetrigclient.trig10"),
    
    GENERATEDEMO("GenerateDemo", "edu.brown.benchmark.voterexperiments.generatedemo"),
    ;


    private final String package_name;
    private final String benchmark_name;

    private ProjectType(String benchmark_name, String package_name) {
        this.benchmark_name = benchmark_name;
        this.package_name = package_name;
    }

    public String getBenchmarkName() {
        return (this.benchmark_name);
    }

    public String getBenchmarkPrefix() {
        return (this.benchmark_name.replace("-", ""));
    }

    /**
     * Returns the package name for where this We need this because we need to
     * be able to dynamically reference various things from the 'src/frontend'
     * directory before we compile the 'tests/frontend' directory
     * 
     * @return
     */
    public String getPackageName() {
        return (this.package_name);
    }

    protected static final Map<Integer, ProjectType> idx_lookup = new HashMap<Integer, ProjectType>();
    protected static final Map<String, ProjectType> name_lookup = new HashMap<String, ProjectType>();
    static {
        for (ProjectType vt : EnumSet.allOf(ProjectType.class)) {
            ProjectType.idx_lookup.put(vt.ordinal(), vt);
            ProjectType.name_lookup.put(vt.name().toLowerCase().intern(), vt);
        } // FOR
    }

    public static ProjectType get(Integer idx) {
        return (ProjectType.idx_lookup.get(idx));
    }

    public static ProjectType get(String name) {
        return (ProjectType.name_lookup.get(name.toLowerCase().intern()));
    }
    
    /**
     * Attempt to find a specific file from the supplemental files directory.
     * @param current
     * @param target_dir
     * @param target_ext
     * @return
     * @throws IOException
     */
    public File getProjectFile(File current, String target_dir, String target_ext) throws IOException {
        boolean has_svn = false;
        for (File file : current.listFiles()) {
            if (file.getCanonicalPath().endsWith("files") && file.isDirectory()) {
                // Look for either a .<target_ext> or a .<target_ext>.gz file
                String file_name = this.name().toLowerCase() + target_ext;
                for (int i = 0; i < 2; i++) {
                    if (i > 0) file_name += ".gz";
                    File target_file = new File(file + File.separator + target_dir + File.separator + file_name);
                    if (target_file.exists() && target_file.isFile()) {
                        return (target_file);
                    }
                } // FOR
                assert(false) : "Unable to find '" + file_name + "' for '" + this + "' in directory '" + file + "'";
            // Make sure that we don't go to far down...
            } else if (file.getCanonicalPath().endsWith("/.svn")) {
                has_svn = true;
            }
        } // FOR
        assert(has_svn) : "Unable to find files directory [last_dir=" + current.getAbsolutePath() + "]";  
        File next = new File(current.getCanonicalPath() + File.separator + "..");
        return (this.getProjectFile(next, target_dir, target_ext));
    }
}

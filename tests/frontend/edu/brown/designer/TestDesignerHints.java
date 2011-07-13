package edu.brown.designer;

import java.io.File;
import java.lang.reflect.Field;

import edu.brown.utils.FileUtil;

import junit.framework.TestCase;

public class TestDesignerHints extends TestCase {

    final DesignerHints hints = new DesignerHints();
    
    public void testSerialization() throws Exception {
        hints.limit_local_time = 1000;
        hints.limit_total_time = 9999;
        hints.ignore_procedures.add("neworder");
        
        File temp = FileUtil.getTempFile("hints", true);
        hints.save(temp.getAbsolutePath());

        DesignerHints clone = new DesignerHints();
        clone.load(temp.getAbsolutePath(), null);
        
        for (Field f : hints.getClass().getFields()) {
            Object expected = f.get(hints);
            Object actual = f.get(clone);
//            System.err.println(String.format("%s => %s / %s", f.getName(), expected, actual));
            assertEquals(f.getName(), expected, actual);
        } // FOR
    }
    
}

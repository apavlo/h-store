package edu.brown.designer;

import java.io.File;
import java.lang.reflect.Field;

import edu.brown.BaseTestCase;
import edu.brown.utils.FileUtil;

public class TestDesignerHints extends BaseTestCase {

    final DesignerHints hints = new DesignerHints();
    
    public void testSerialization() throws Exception {
        hints.limit_local_time = 1000;
        hints.limit_total_time = 9999;
        hints.ignore_procedures.add("neworder");
        
        File temp = FileUtil.getTempFile("hints", true);
        System.err.println(temp);
        hints.save(temp);

        DesignerHints clone = new DesignerHints();
        clone.load(temp, null);
        
        for (Field f : hints.getClass().getFields()) {
            Object expected = f.get(hints);
            Object actual = f.get(clone);
//            System.err.println(String.format("%s => %s / %s", f.getName(), expected, actual));
            assertEquals(f.getName(), expected, actual);
        } // FOR
    }
    
}

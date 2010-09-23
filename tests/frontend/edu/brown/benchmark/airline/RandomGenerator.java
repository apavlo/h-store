/**
 * 
 */
package edu.brown.benchmark.airline;

import edu.brown.rand.AbstractRandomGenerator;

/**
 * @author pavlo
 *
 */
public class RandomGenerator extends AbstractRandomGenerator {

    public RandomGenerator(Integer seed) {
        super(seed);
    }
    
    /* (non-Javadoc)
     * @see org.voltdb.benchmark.AbstractRandomGenerator#loadProfile(java.lang.String)
     */
    @Override
    public void loadProfile(String inputPath) throws Exception {
        // TODO Auto-generated method stub

    }
    
    @Override
    public int numberExcluding(int minimum, int maximum, int excluding, String sourceTable, String targetTable) {
        return (this.numberExcluding(minimum, maximum, excluding));
    }
    
    @Override
    public int numberAffinity(int minimum, int maximum, int base, String baseTable, String targetTable) {
        // TODO Auto-generated method stub
        return 0;
    }


    /* (non-Javadoc)
     * @see org.voltdb.benchmark.AbstractRandomGenerator#numberAffinity(int, int, int, java.lang.String)
     */
//    @Override
//    public int numberAffinity(int minimum, int maximum, int base, String table) {
//        // TODO Auto-generated method stub
//        return 0;
//    }

    /* (non-Javadoc)
     * @see org.voltdb.benchmark.AbstractRandomGenerator#numberExcluding(int, int, int, java.lang.String)
     */
//    @Override
//    public int numberExcluding(int minimum, int maximum, int excluding,
//            String table) {
//        // TODO Auto-generated method stub
//        return 0;
//    }

    /* (non-Javadoc)
     * @see org.voltdb.benchmark.AbstractRandomGenerator#numberSkewed(int, int, double)
     */
    @Override
    public int numberSkewed(int minimum, int maximum, double skewFactor) {
        // TODO Auto-generated method stub
        return 0;
    }

    /* (non-Javadoc)
     * @see org.voltdb.benchmark.AbstractRandomGenerator#saveProfile(java.lang.String)
     */
    @Override
    public void saveProfile(String outputPath) throws Exception {
        // TODO Auto-generated method stub

    }

}

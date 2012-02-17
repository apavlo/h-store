package edu.brown.rand;

/**
 * @author pavlo
 */
public class DefaultRandomGenerator extends AbstractRandomGenerator {
    private static final long serialVersionUID = 1L;

    /**
     * @param seed
     */
    public DefaultRandomGenerator() {
        super();
    }

    /**
     * @param seed
     */
    public DefaultRandomGenerator(Integer seed) {
        super(seed);
    }

    /*
     * (non-Javadoc)
     * @see edu.brown.rand.AbstractRandomGenerator#loadProfile(java.lang.String)
     */
    @Override
    public void loadProfile(String input_path) throws Exception {
        // TODO Auto-generated method stub

    }

    /*
     * (non-Javadoc)
     * @see edu.brown.rand.AbstractRandomGenerator#numberAffinity(int, int, int,
     * java.lang.String, java.lang.String)
     */
    @Override
    public int numberAffinity(int minimum, int maximum, int base, String base_table, String target_table) {
        // TODO Auto-generated method stub
        return 0;
    }

    /*
     * (non-Javadoc)
     * @see edu.brown.rand.AbstractRandomGenerator#numberExcluding(int, int,
     * int, java.lang.String, java.lang.String)
     */
    @Override
    public int numberExcluding(int minimum, int maximum, int excluding, String source_table, String target_table) {
        // TODO Auto-generated method stub
        return 0;
    }

    /*
     * (non-Javadoc)
     * @see edu.brown.rand.AbstractRandomGenerator#saveProfile(java.lang.String)
     */
    @Override
    public void saveProfile(String output_path) throws Exception {
        // TODO Auto-generated method stub

    }

}

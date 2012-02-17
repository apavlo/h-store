package edu.brown.rand;

import edu.brown.rand.RandomDistribution.DiscreteRNG;

/**
 * @author pavlo
 */
public class WrappingRandomDistribution extends DiscreteRNG {

    private DiscreteRNG rng;
    private final long start;

    /**
     * Constructor
     * 
     * @param rng
     * @param start
     */
    public WrappingRandomDistribution(DiscreteRNG rng, long start) {
        super(rng.getRandom(), rng.getMin(), rng.getMax());

        assert (start <= this.max);
        assert (start >= this.min);

        this.rng = rng;
        this.start = start;
    }

    @Override
    protected long nextLongImpl() {
        long next = this.rng.nextLong();
        next += this.start;
        if (next >= this.max) {
            long delta = next - this.max;
            next = this.min + delta;
        }
        return (next);
    }
}

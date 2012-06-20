package edu.brown.api;

import java.io.IOException;

public abstract class Loader extends BenchmarkComponent {

    public Loader(String[] args) {
        super(args);
    }
    
    public abstract void load() throws IOException;
    
    @Override
    protected final void runLoop() throws IOException {
        this.load();
    }

    @Override
    protected final String[] getTransactionDisplayNames() {
        // Not needed here
        return null;
    }

}

package edu.brown.benchmark.upsert;

import edu.brown.api.Loader;

public class UpsertLoader extends Loader {

    public static void main(String args[]) throws Exception {
        Loader.main(UpsertLoader.class, args, true);
    }

    public UpsertLoader(String[] args) {
        super(args);
    }

    @Override
    public void load() {

        try {
            this.getClientHandle().callProcedure("Initialize",
                                                 10);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}

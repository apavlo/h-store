package edu.brown.utils;

import org.voltdb.utils.Pair;

public class EventObservableExceptionHandler extends EventObservable<Pair<Thread, Throwable>> implements Thread.UncaughtExceptionHandler {

    private Throwable error;
    
    @Override
    public void uncaughtException(Thread t, Throwable e) {
        if (this.error == null) this.error = e;
        this.notifyObservers(Pair.of(t, e));
    }
    
    public boolean hasError() {
        return (this.error != null);
    }
    
    public Throwable getError() {
        return (this.error);
    }

}

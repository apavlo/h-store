package edu.brown.utils;

public class EventObservableExceptionHandler extends EventObservable implements Thread.UncaughtExceptionHandler {

    private Throwable error;
    
    @Override
    public void uncaughtException(Thread t, Throwable e) {
        if (this.error == null) this.error = e;
        this.notifyObservers(e);
    }
    
    public boolean hasError() {
        return (this.error != null);
    }
    
    public Throwable getError() {
        return (this.error);
    }

}

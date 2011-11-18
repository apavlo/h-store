package edu.brown.utils;

import java.util.Observable;

/**
 * EventObservable
 * 
 */
public class EventObservable<T> {

    protected class InnerObservable extends Observable {
        @Override
        public synchronized void setChanged() {
            super.setChanged();
        }
        public EventObservable<T> getEventObservable() {
            return (EventObservable.this);
        }
    };
    
    private final InnerObservable observable;
    private int observer_ctr = 0;
    
    public EventObservable() {
        this.observable = new InnerObservable();
    }

    public synchronized void addObserver(EventObserver<T> o) {
        if (o != null) {
            this.observable.addObserver(o.getObserver());
            this.observer_ctr++;
        }
    }

    public synchronized void deleteObserver(EventObserver<T> o) {
        this.observable.deleteObserver(o.getObserver());
        this.observer_ctr--;
    }
    
    public synchronized void deleteObservers() {
        this.observable.deleteObservers();
        this.observer_ctr = 0;
    }
    
    public int countObservers() {
        return (this.observer_ctr);
    }

    /**
     * Notifies the Observers that a changed occurred
     * @param arg - the state that changed
     */
    public void notifyObservers(T arg) {
        this.observable.setChanged();
        if (this.observer_ctr > 0)
            this.observable.notifyObservers(arg);
    }

    /**
     * Notifies the Observers that a changed occurred
     */
    public void notifyObservers() {
        this.observable.setChanged();
        if (this.observer_ctr > 0)
            this.observable.notifyObservers();
    }
} // END CLASS
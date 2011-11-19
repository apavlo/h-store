package edu.brown.utils;

import java.util.*;

/**
 * EventObservable
 */
public abstract class EventObserver<T> {

    protected class InnerObserver implements Observer {
        @SuppressWarnings("unchecked")
        @Override
        public void update(Observable o, Object arg) {
            assert(o instanceof EventObservable<?>.InnerObservable);
            EventObserver.this.update(((EventObservable.InnerObservable)o).getEventObservable(), (T)arg);
        }
        public EventObserver<T> getEventObserver() {
            return (EventObserver.this);
        }
    }
    
    private final InnerObserver observer;
    
    public EventObserver() {
        this.observer = new InnerObserver();
    }
    
    protected Observer getObserver() {
        return (this.observer);
    }

    public abstract void update(EventObservable<T> o, T arg);
}
package edu.brown.utils;

import java.util.Observable;
import java.util.Observer;

/**
 *  EventObservable
 *
 */
public class EventObservable extends Observable {

    private int observer_ctr = 0;
    
    @Override
    public synchronized void addObserver(Observer o) {
        if (o != null) {
            super.addObserver(o);
            this.observer_ctr++;
        }
    }
    
    @Override
    public synchronized void deleteObserver(Observer o) {
        super.deleteObserver(o);
        this.observer_ctr--;
    }
    
    /********************************************************
     * notifyObservers()
     * Notifies the Observers that a changed occured
     *
     * @param arg - the state that changed
     ********************************************************/
    public void notifyObservers(Object arg) {
        this.setChanged();
        if (this.observer_ctr > 0) super.notifyObservers(arg);
    }
   
    /********************************************************
     * notifyObservers()
     * Notifies the Observers that a changed occured
     *
     ********************************************************/
    public void notifyObservers() {
        this.setChanged();
        if (this.observer_ctr > 0) super.notifyObservers();
    }
} // END CLASS
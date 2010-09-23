package edu.brown.utils;

import java.util.Observable;

/**
 *  EventObservable
 *
 */
public class EventObservable extends Observable {

    /********************************************************
     * notifyObservers()
     * Notifies the Observers that a changed occured
     *
     * @param arg - the state that changed
     ********************************************************/
    public void notifyObservers(Object arg) {
        this.setChanged();
        super.notifyObservers(arg);
    }
   
    /********************************************************
     * notifyObservers()
     * Notifies the Observers that a changed occured
     *
     ********************************************************/
    public void notifyObservers() {
        this.setChanged();
        super.notifyObservers();
    }
} // END CLASS
package edu.brown.gui.common;

import java.awt.event.MouseEvent;

import edu.uci.ics.jung.visualization.control.GraphMouseListener;
import edu.uci.ics.jung.visualization.picking.PickedState;

/**
 * 
 * @author pavlo
 *
 * @param <T>
 */
public class MouseListener<T> implements GraphMouseListener<T> {
    private T selected = null;
    private final PickedState<T> picked;
    
    public MouseListener(PickedState<T> picked) {
        this.picked = picked;
    }
    
    public void graphClicked(T element, MouseEvent event) {
        if (element == null) {
//            System.out.println("UNLICK!");
            this.picked.pick(this.selected, false);
        } else {
            if (this.selected != element) {
                if (this.selected != null) {
                    this.picked.pick(this.selected, false);
                }
                this.selected = element;
                this.picked.pick(this.selected, true);
                //System.err.println("CLICKED: " + element);
            }
        }
    }
    public void graphPressed(T element, MouseEvent event) {
        //System.err.println("PRESSED: " + v);
    }
    public void graphReleased(T element, MouseEvent event) {
        //System.err.println("RELEASED: " + v);
    }
} // END CLASS

package edu.brown.gui;

import javax.swing.JPanel;

import org.apache.log4j.Logger;

public abstract class AbstractInfoPanel<T> extends JPanel {
    protected static final Logger LOG = Logger.getLogger(AbstractInfoPanel.class.getName());
    private static final long serialVersionUID = 1L;
    protected T element;
    
    public AbstractInfoPanel() {
        super();
        try {
            this.init();
        } catch (Exception ex) {
            ex.printStackTrace();
            System.exit(1);
        }
    }
    
    public T getElement() {
        return this.element;
    }
    
    public abstract void update(T element);
    protected abstract void init() throws Exception;

}

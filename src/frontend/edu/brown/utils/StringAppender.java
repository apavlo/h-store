/***************************************************************************
 *  Copyright (C) 2012 by H-Store Project                                  *
 *  Brown University                                                       *
 *  Massachusetts Institute of Technology                                  *
 *  Yale University                                                        *
 *                                                                         *
 *  http://hstore.cs.brown.edu/                                            *
 *                                                                         *
 *  Permission is hereby granted, free of charge, to any person obtaining  *
 *  a copy of this software and associated documentation files (the        *
 *  "Software"), to deal in the Software without restriction, including    *
 *  without limitation the rights to use, copy, modify, merge, publish,    *
 *  distribute, sublicense, and/or sell copies of the Software, and to     *
 *  permit persons to whom the Software is furnished to do so, subject to  *
 *  the following conditions:                                              *
 *                                                                         *
 *  The above copyright notice and this permission notice shall be         *
 *  included in all copies or substantial portions of the Software.        *
 *                                                                         *
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,        *
 *  EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF     *
 *  MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. *
 *  IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR      *
 *  OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,  *
 *  ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR  *
 *  OTHER DEALINGS IN THE SOFTWARE.                                        *
 ***************************************************************************/
/**
 * 
 */
package edu.brown.utils;

import org.apache.log4j.Appender;
import org.apache.log4j.Layout;
import org.apache.log4j.spi.ErrorHandler;
import org.apache.log4j.spi.Filter;
import org.apache.log4j.spi.LoggingEvent;

/**
 * @author pavlo
 */
public class StringAppender implements Appender {

    private StringBuilder sb;
    private String name;

    /**
     * 
     */
    public StringAppender() {
        this.sb = new StringBuilder();
    }

    /*
     * (non-Javadoc)
     * @see
     * org.apache.log4j.Appender#doAppend(org.apache.log4j.spi.LoggingEvent)
     */
    @Override
    public void doAppend(LoggingEvent arg0) {
        this.sb.append(arg0.getMessage()).append("\n");
    }

    public void clear() {
        this.sb = new StringBuilder();
    }

    @Override
    public String toString() {
        return this.sb.toString();
    }

    /*
     * (non-Javadoc)
     * @see org.apache.log4j.Appender#addFilter(org.apache.log4j.spi.Filter)
     */
    @Override
    public void addFilter(Filter arg0) {
        // TODO Auto-generated method stub
    }

    /*
     * (non-Javadoc)
     * @see org.apache.log4j.Appender#clearFilters()
     */
    @Override
    public void clearFilters() {
        // TODO Auto-generated method stub
    }

    /*
     * (non-Javadoc)
     * @see org.apache.log4j.Appender#close()
     */
    @Override
    public void close() {
        // TODO Auto-generated method stub
    }

    /*
     * (non-Javadoc)
     * @see org.apache.log4j.Appender#getErrorHandler()
     */
    @Override
    public ErrorHandler getErrorHandler() {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * @see org.apache.log4j.Appender#getFilter()
     */
    @Override
    public Filter getFilter() {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * @see org.apache.log4j.Appender#getLayout()
     */
    @Override
    public Layout getLayout() {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * @see org.apache.log4j.Appender#getName()
     */
    @Override
    public String getName() {
        return this.name;
    }

    /*
     * (non-Javadoc)
     * @see org.apache.log4j.Appender#requiresLayout()
     */
    @Override
    public boolean requiresLayout() {
        // TODO Auto-generated method stub
        return false;
    }

    /*
     * (non-Javadoc)
     * @see
     * org.apache.log4j.Appender#setErrorHandler(org.apache.log4j.spi.ErrorHandler
     * )
     */
    @Override
    public void setErrorHandler(ErrorHandler arg0) {
        // TODO Auto-generated method stub
    }

    /*
     * (non-Javadoc)
     * @see org.apache.log4j.Appender#setLayout(org.apache.log4j.Layout)
     */
    @Override
    public void setLayout(Layout arg0) {
        // TODO Auto-generated method stub
    }

    /*
     * (non-Javadoc)
     * @see org.apache.log4j.Appender#setName(java.lang.String)
     */
    @Override
    public void setName(String arg0) {
        this.name = arg0;
    }
}

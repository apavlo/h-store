/* This file is part of VoltDB.
 * Copyright (C) 2008-2009 Vertica Systems Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package edu.brown.expressions;

import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.InComparisonExpression;

import edu.brown.utils.AbstractTreeWalker;

/**
 * Utility class to help with walking through Note that this does operations in
 * a DEPTH-FIRST FASHION
 */
public abstract class ExpressionTreeWalker extends AbstractTreeWalker<AbstractExpression> {

    @Override
    protected final void populate_children(ExpressionTreeWalker.Children<AbstractExpression> children, AbstractExpression element) {
        //
        // This is kind of screwy, so bare with me here for second...
        // We actually want to do a real depth first search, and not visit all
        // the children
        // before we get to the parent.
        //
        if (element.getLeft() != null) {
            children.addBefore(element.getLeft());
        }
        if (element.getRight() != null) {
            children.addAfter(element.getRight());
        }
        //
        // If this is an InComparisonExpression, then we need to traverse all
        // of its children too. This should replace the right expression
        //
        if (element instanceof InComparisonExpression) {
            for (AbstractExpression child : ((InComparisonExpression) element).getValues()) {
                children.addAfter(child);
            } // FOR
        }
        return;
    }
} // END CLASS
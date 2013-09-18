/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

// added by hawk, 9/18/2013
package org.voltdb;

/**
 * real Trigger extend VoltTrigger to
 * create work in the system. This functionality is not available to standard
 * user procedures (which extend VoltProcedure).
 */
public abstract class VoltTrigger extends VoltProcedure {
	private String streamname = null;
	
	public VoltTrigger() {
		setStreamName();
    } 

	public void setStreamName() {
		this.streamname = toSetStreamName();
	}

	protected abstract String toSetStreamName() ;

	public String getStreamName() {
		return streamname;
	}
}

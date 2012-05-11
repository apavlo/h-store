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

package org.voltdb.plannodes;

import org.json.JSONException;
import org.json.JSONObject;
import org.voltdb.catalog.Database;
import org.voltdb.planner.PlannerContext;
import org.voltdb.types.PlanNodeType;

/**
 *
 */
public class ReceivePlanNode extends AbstractPlanNode {

    /**
     * @param id
     */
	
	private boolean m_fast = false; // determine if it will be fast executed --mimosally
	private boolean fastcombine=false; //determine if it will be fast combine

    public void setFast(boolean fast) {
    	m_fast = fast;
    }
    public boolean getFast(){
    	return m_fast;
    }
    public ReceivePlanNode(PlannerContext context, Integer id) {
        super(context, id);
    }

    @Override
    public PlanNodeType getPlanNodeType() {
        return PlanNodeType.RECEIVE;
    }

    @Override
    public void validate() throws Exception {
        super.validate();
    }

    @Override
    protected void loadFromJSONObject(JSONObject obj, Database db) throws JSONException {
    }
	public boolean getFastcombine() {
		return fastcombine;
	}
	public void setFastcombine(boolean fastcombine) {
		this.fastcombine = fastcombine;
	}
}

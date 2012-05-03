/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB Inc.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by VoltDB Inc. are licensed under the following
 * terms and conditions:
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
/* Copyright (C) 2008 by H-Store Project
 * Brown University
 * Massachusetts Institute of Technology
 * Yale University
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

#include "abstractjoinnode.h"

#include "expressions/abstractexpression.h"

#include <stdexcept>

using namespace json_spirit;
using namespace std;
using namespace voltdb;

AbstractJoinPlanNode::AbstractJoinPlanNode(CatalogId id)
    : AbstractPlanNode(id), m_predicate(NULL)
{
}

AbstractJoinPlanNode::AbstractJoinPlanNode()
    : AbstractPlanNode(), m_predicate(NULL)
{
}

AbstractJoinPlanNode::~AbstractJoinPlanNode()
{
    delete m_predicate;
}

JoinType AbstractJoinPlanNode::getJoinType() const
{
    return m_joinType;
}

void AbstractJoinPlanNode::setJoinType(JoinType join_type)
{
    m_joinType = join_type;
}

void AbstractJoinPlanNode::setPredicate(AbstractExpression* predicate)
{
    assert(!m_predicate);
    if (m_predicate != predicate)
    {
        delete m_predicate;
    }
    m_predicate = predicate;
}

AbstractExpression* AbstractJoinPlanNode::getPredicate() const
{
    return m_predicate;
}

string AbstractJoinPlanNode::debugInfo(const string& spacer) const
{
    ostringstream buffer;
    buffer << spacer << "JoinType[" << m_joinType << "]\n";
    if (m_predicate != NULL)
    {
        buffer << m_predicate->debug(spacer);
    }
    
    buffer << spacer << "OutputColumns[" << m_outputColumnGuids.size() << "]\n";
    for (int ii = 0; ii < m_outputColumnGuids.size(); ii++) {
        buffer << spacer << "  [" << ii << "] " << m_outputColumnGuids[ii] << "\n";
    }
    
    return (buffer.str());
}

int
AbstractJoinPlanNode::getColumnIndexFromGuid(int guid,
                                             const catalog::Database* db) const
{
    for (int ii = 0; ii < m_outputColumnGuids.size(); ii++)
    {
        if (m_outputColumnGuids[ii] == guid)
        {
            return ii;
        }
    }
    return -1;
}

void
AbstractJoinPlanNode::setOutputColumnGuids(vector<int> output_column_guids)
{
    m_outputColumnGuids = output_column_guids;
}

void
AbstractJoinPlanNode::loadFromJSONObject(Object& obj,
                                         const catalog::Database* catalog_db)
{
    Value joinTypeValue = find_value(obj, "JOIN_TYPE");
    if (joinTypeValue == Value::null)
    {
        throw SerializableEEException(VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION,
                                      "AbstractJoinPlanNode::loadFromJSONObject:"
                                      " Couldn't find JOIN_TYPE value");
    }
    m_joinType = stringToJoin(joinTypeValue.get_str());

    Value predicateValue = find_value(obj, "PREDICATE");
    if (predicateValue == Value::null)
    {
        m_predicate = NULL;
    }
    else
    {
        Object predicateObject = predicateValue.get_obj();
        m_predicate = AbstractExpression::buildExpressionTree(predicateObject);
    }
}

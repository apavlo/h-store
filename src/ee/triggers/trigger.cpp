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

//#include <sstream>
//#include <cassert>
//#include <cstdio>
#include "triggers/trigger.h"
#include "common/types.h"
#include "storage/table.h"
#include "catalog/statement.h"
#include "catalog/catalogmap.h"
#include <sys/time.h>
#include <time.h>
#include <cassert>
#include "triggers/TriggerStats.h"

using std::map;
using std::string;
using std::vector;

namespace voltdb {

Trigger::~Trigger() {
	VOLT_DEBUG("TRIGGER DESTRUCTOR");
	delete m_frags;
}

Trigger::Trigger(const int32_t id, const string &name, const catalog::CatalogMap<catalog::Statement> *stmts, unsigned char type, bool forEach) :
	m_id(id), m_name(name), m_type(type), m_forEach(forEach), stats_(this)
{
	VOLT_DEBUG("TRIGGER CONSTRUCTOR");
	m_latency = 0;

	m_frags = new vector<const catalog::PlanFragment*>;
	map<string, catalog::Statement *>::const_iterator stmt_iter = stmts->begin();
	for( ; stmt_iter != stmts->end(); stmt_iter++){
		const catalog::Statement * curstmt = stmt_iter->second;
		map<string, catalog::PlanFragment*>::const_iterator frag_iter;
		//fragments loop
		for(frag_iter = curstmt->fragments().begin();
				frag_iter != curstmt->fragments().end(); frag_iter++){
			m_frags->push_back(frag_iter->second);
		}
	}
}

int32_t Trigger::id() const {
	return m_id;
}

string Trigger::name() const {
    return m_name;
}

int64_t Trigger::latency() const {
	return m_latency;
}

voltdb::TriggerStats* Trigger::getTriggerStats() {
	return &stats_;
}

int64_t timespecDiffNanoseconds(const timespec& end, const timespec& start) {
    assert(timespecValid(end));
    assert(timespecValid(start));
    return (end.tv_nsec - start.tv_nsec) + (end.tv_sec - start.tv_sec) * (int64_t) 1000000000;
}

void Trigger::fire(VoltDBEngine *engine, Table *input) {
	struct timeval start_;
	struct timeval end_;
	struct timespec start;
	struct timespec end;


	int error = gettimeofday(&start_, NULL);
        assert(error == 0);
	// TODO: Loop through all the single-partition plan fragment ids for
	// the target Statement. Then for each of them, invoke engine->executeQueryNoOutput()
	// Throw an exception if something bad happens...
	int64_t txnId = engine->getExecutorContext()->currentTxnId();
	bool send_tuple_count = false;
	const NValueArray params;
	VOLT_DEBUG("FIRE 1");
	//assert(m_frags != NULL);
	//assert(m_frags.size() != 0);
	VOLT_DEBUG("m_frag size %d", int(m_frags->size()));
	VOLT_DEBUG("FIRE 2");
	vector<const catalog::PlanFragment *>::const_iterator frag_iter = m_frags->begin();
	VOLT_DEBUG("FIRE 3");

	for( ;	frag_iter != m_frags->end(); frag_iter++){
		VOLT_DEBUG("FIRE 4");
		int64_t planfragmentId = (int64_t)((*frag_iter)->id());
		engine->executeQueryNoOutput(planfragmentId, params, txnId, send_tuple_count);
		VOLT_DEBUG("FIRE 5");
		send_tuple_count = false;
	}

	error = gettimeofday(&end_, NULL);
       assert(error == 0);

	TIMEVAL_TO_TIMESPEC(&start_, &start);
	TIMEVAL_TO_TIMESPEC(&end_, &end);
	m_latency = timespecDiffNanoseconds(end, start);
}

bool Trigger::setType(unsigned char t) {
	// TODO: Change to type. Setup in types.h
	if(t != TRIGGER_INSERT && t != TRIGGER_UPDATE && t != TRIGGER_DELETE)
		return false;
	else{
		m_type = t;
		return true;
	}
}

void Trigger::setForEach(bool fe){
	m_forEach = fe;
}

void Trigger::setSourceTable(Table *t){
	m_sourceTable = t;
}

//const catalog::CatalogMap<catalog::Statement> & Trigger::getStatements(){
//	return m_statements;
//}

unsigned char Trigger::getType(){
	return m_type;
}

bool Trigger::getForEach(){
	return m_forEach;
}

Table *Trigger::getSourceTable(){
	return m_sourceTable;
}

}

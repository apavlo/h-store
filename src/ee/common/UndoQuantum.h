/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB Inc.
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

#ifndef UNDOQUANTUM_H_
#define UNDOQUANTUM_H_

#include <vector>
#include <stdint.h>
#include <cassert>

#include "common/Pool.hpp"
#include "common/UndoAction.h"

namespace voltdb {

class UndoQuantum {
public:
    inline UndoQuantum(int64_t undoToken, Pool *dataPool)
        : m_undoToken(undoToken), m_dataPool(dataPool) {}
    inline virtual ~UndoQuantum() {}

    virtual inline void registerUndoAction(UndoAction *undoAction) {
        assert(undoAction);
        m_undoActions.push_back(undoAction);
    }

    /*
     * Invoke all the undo actions for this UndoQuantum. UndoActions
     * must have released all memory after undo() is called. Their
     * destructor will never be called because they are allocated out
     * of the data pool which will be purged in one go.
     */
    inline void undo() {
        for (std::vector<UndoAction*>::reverse_iterator i = m_undoActions.rbegin();
             i != m_undoActions.rend(); i++) {
            (*i)->undo();
            (*i)->~UndoAction();
        }
        this->~UndoQuantum();
    }

    /*
     * Call the destructors of all the UndoActions for this
     * UndoQuantum so they will release any resources they still hold.
     * Also call own destructor to ensure that the vector is released.
     */
    inline void release() {
        for (std::vector<UndoAction*>::reverse_iterator i = m_undoActions.rbegin();
             i != m_undoActions.rend(); i++) {
            (*i)->release();
            (*i)->~UndoAction();
        }
        this->~UndoQuantum();
    }

    inline int64_t getUndoToken() const {
        return m_undoToken;
    }

    virtual inline Pool* getDataPool() {
        return m_dataPool;
    }

    virtual bool isDummy() {return false;}

private:
    const int64_t m_undoToken;
    std::vector<UndoAction*> m_undoActions;
protected:
    Pool *m_dataPool;
};

}

#endif /* UNDOQUANTUM_H_ */

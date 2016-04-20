/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002, 2015 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package com.sleepycat.util.keyrange;

import com.sleepycat.compat.DbCompat;
import com.sleepycat.db.Cursor;
import com.sleepycat.db.DatabaseEntry;
import com.sleepycat.db.DatabaseException;
import com.sleepycat.db.LockMode;
import com.sleepycat.db.OperationStatus;
import com.sleepycat.db.SecondaryCursor;

/**
 * A cursor-like interface that enforces a key range.  The method signatures
 * are actually those of SecondaryCursor, but the pKey parameter may be null.
 * It was done this way to avoid doubling the number of methods.
 *
 * <p>This is not a fully general implementation of a range cursor and should
 * not be used directly by applications; however, it may evolve into a
 * generally useful range cursor some day.</p>
 *
 * @author Mark Hayes
 */
public class RangeCursor implements Cloneable {

    /**
     * The cursor and secondary cursor are the same object.  The secCursor is
     * null if the database is not a secondary database.
     */
    private Cursor cursor;
    private SecondaryCursor secCursor;

    /**
     * The range is always non-null, but may be unbounded meaning that it is
     * open and not used.
     */
    private KeyRange range;

    /**
     * The pkRange may be non-null only if the range is a single-key range
     * and the cursor is a secondary cursor.  It further restricts the range of
     * primary keys in a secondary database.
     */
    private KeyRange pkRange;

    /**
     * If the DB supported sorted duplicates, then calling
     * Cursor.getSearchBothRange is allowed.
     */
    private boolean sortedDups;

    /**
     * The privXxx entries are used only when the range is bounded.  We read
     * into these private entries to avoid modifying the caller's entry
     * parameters in the case where we read successfully but the key is out of
     * range.  In that case we return NOTFOUND and we want to leave the entry
     * parameters unchanged.
     */
    private DatabaseEntry privKey;
    private DatabaseEntry privPKey;
    private DatabaseEntry privData;

    /**
     * The initialized flag is set to true whenever we successfully position
     * the cursor.  It is used to implement the getNext/Prev logic for doing a
     * getFirst/Last when the cursor is not initialized.  We can't rely on
     * Cursor to do that for us, since if we position the underlying cursor
     * successfully but the key is out of range, we have no way to set the
     * underlying cursor to uninitialized.  A range cursor always starts in the
     * uninitialized state.
     */
    private boolean initialized;

    /**
     * Creates a range cursor with a duplicate range.
     */
    public RangeCursor(KeyRange range,
                       KeyRange pkRange,
                       boolean sortedDups,
                       Cursor cursor) {
        if (pkRange != null && !range.singleKey) {
            throw new IllegalArgumentException();
        }
        this.range = range;
        this.pkRange = pkRange;
        this.sortedDups = sortedDups;
        this.cursor = cursor;
        init();
        if (pkRange != null && secCursor == null) {
            throw new IllegalArgumentException();
        }
    }

    /**
     * Create a cloned range cursor.  The caller must clone the underlying
     * cursor before using this constructor, because cursor open/close is
     * handled specially for CDS cursors outside this class.
     */
    public RangeCursor dup(boolean samePosition)
        throws DatabaseException {

        try {
            RangeCursor c = (RangeCursor) super.clone();
            c.cursor = dupCursor(cursor, samePosition);
            c.init();
            return c;
        } catch (CloneNotSupportedException neverHappens) {
            return null;
        }
    }

    /**
     * Used for opening and duping (cloning).
     */
    private void init() {

        if (cursor instanceof SecondaryCursor) {
            secCursor = (SecondaryCursor) cursor;
        } else {
            secCursor = null;
        }

        if (range.hasBound()) {
            privKey = new DatabaseEntry();
            privPKey = new DatabaseEntry();
            privData = new DatabaseEntry();
        } else {
            privKey = null;
            privPKey = null;
            privData = null;
        }
    }

    /**
     * Returns whether the cursor is initialized at a valid position.
     */
    public boolean isInitialized() {
        return initialized;
    }

    /**
     * Returns the underlying cursor.  Used for cloning.
     */
    public Cursor getCursor() {
        return cursor;
    }

    /**
     * When an unbounded range is used, this method is called to use the
     * callers entry parameters directly, to avoid the extra step of copying
     * between the private entries and the caller's entries.
     */
    private void setParams(DatabaseEntry key,
                           DatabaseEntry pKey,
                           DatabaseEntry data) {
        privKey = key;
        privPKey = pKey;
        privData = data;
    }

    /**
     * Dups the cursor, sets the cursor and secCursor fields to the duped
     * cursor, and returns the old cursor.  Always call endOperation in a
     * finally clause after calling beginOperation.
     *
     * <p>If the returned cursor == the cursor field, the cursor is
     * uninitialized and was not duped; this case is handled correctly by
     * endOperation.</p>
     */
    private Cursor beginOperation()
        throws DatabaseException {

        Cursor oldCursor = cursor;
        if (initialized) {
            cursor = dupCursor(cursor, true);
            if (secCursor != null) {
                secCursor = (SecondaryCursor) cursor;
            }
        } else {
            return cursor;
        }
        return oldCursor;
    }

    /**
     * If the operation succeeded, leaves the duped cursor in place and closes
     * the oldCursor.  If the operation failed, moves the oldCursor back in
     * place and closes the duped cursor.  oldCursor may be null if
     * beginOperation was not called, in cases where we don't need to dup
     * the cursor.  Always call endOperation when a successful operation ends,
     * in order to set the initialized field.
     */
    private void endOperation(Cursor oldCursor,
                              OperationStatus status,
                              DatabaseEntry key,
                              DatabaseEntry pKey,
                              DatabaseEntry data)
        throws DatabaseException {

        if (status == OperationStatus.SUCCESS) {
            if (oldCursor != null && oldCursor != cursor) {
                closeCursor(oldCursor);
            }
            if (key != null) {
                swapData(key, privKey);
            }
            if (pKey != null && secCursor != null) {
                swapData(pKey, privPKey);
            }
            if (data != null) {
                swapData(data, privData);
            }
            initialized = true;
        } else {
            if (oldCursor != null && oldCursor != cursor) {
                closeCursor(cursor);
                cursor = oldCursor;
                if (secCursor != null) {
                    secCursor = (SecondaryCursor) cursor;
                }
            }
        }
    }

    /**
     * Swaps the contents of the two entries.  Used to return entry data to
     * the caller when the operation was successful.
     */
    private static void swapData(DatabaseEntry e1, DatabaseEntry e2) {

        byte[] d1 = e1.getData();
        int o1 = e1.getOffset();
        int s1 = e1.getSize();

        e1.setData(e2.getData(), e2.getOffset(), e2.getSize());
        e2.setData(d1, o1, s1);
    }

    /**
     * Shares the same byte array, offset and size between two entries.
     * Used when copying the entry data is not necessary because it is known
     * that the underlying operation will not modify the entry, for example,
     * with getSearchKey.
     */
    private static void shareData(DatabaseEntry from, DatabaseEntry to) {

        if (from != null) {
            to.setData(from.getData(), from.getOffset(), from.getSize());
        }
    }

    public OperationStatus getFirst(DatabaseEntry key,
                                    DatabaseEntry pKey,
                                    DatabaseEntry data,
                                    LockMode lockMode)
        throws DatabaseException {

        OperationStatus status;
        if (!range.hasBound()) {
            setParams(key, pKey, data);
            status = doGetFirst(lockMode);
            endOperation(null, status, null, null, null);
            return status;
        }
        if (pkRange != null && pkRange.isSingleKey()) {
            KeyRange.copy(range.beginKey, privKey);
            KeyRange.copy(pkRange.beginKey, privPKey);
            status = doGetSearchBoth(lockMode);
            endOperation(null, status, key, pKey, data);
            return status;
        }
        if (pkRange != null) {
            KeyRange.copy(range.beginKey, privKey);
            status = OperationStatus.NOTFOUND;
            Cursor oldCursor = beginOperation();
            try {
                if (pkRange.beginKey == null || !sortedDups) {
                    status = doGetSearchKey(lockMode);
                } else {
                    KeyRange.copy(pkRange.beginKey, privPKey);
                    status = doGetSearchBothRange(lockMode);
                    if (status == OperationStatus.SUCCESS &&
                        !pkRange.beginInclusive &&
                        pkRange.compare(privPKey, pkRange.beginKey) == 0) {
                        status = doGetNextDup(lockMode);
                    }
                }
                if (status == OperationStatus.SUCCESS &&
                    !pkRange.check(privPKey)) {
                    status = OperationStatus.NOTFOUND;
                }
            } finally {
                endOperation(oldCursor, status, key, pKey, data);
            }
        } else if (range.singleKey) {
            KeyRange.copy(range.beginKey, privKey);
            status = doGetSearchKey(lockMode);
            endOperation(null, status, key, pKey, data);
        } else {
            status = OperationStatus.NOTFOUND;
            Cursor oldCursor = beginOperation();
            try {
                if (range.beginKey == null) {
                    status = doGetFirst(lockMode);
                } else {
                    KeyRange.copy(range.beginKey, privKey);
                    status = doGetSearchKeyRange(lockMode);
                    if (status == OperationStatus.SUCCESS &&
                        !range.beginInclusive &&
                        range.compare(privKey, range.beginKey) == 0) {
                        status = doGetNextNoDup(lockMode);
                    }
                }
                if (status == OperationStatus.SUCCESS &&
                    !range.check(privKey)) {
                    status = OperationStatus.NOTFOUND;
                }
            } finally {
                endOperation(oldCursor, status, key, pKey, data);
            }
        }
        return status;
    }

    /**
     * This method will restart the operation when a key range is used and an
     * insertion at the end of the key range is performed in another thread.
     * The restarts are needed because a sequence of cursor movements is
     * performed, and serializable isolation cannot be relied on to prevent
     * insertions in other threads. Without the restarts, getLast could return
     * NOTFOUND when keys in the range exist. This may only be an issue for JE
     * since it uses record locking, while DB core uses page locking.
     */
    public OperationStatus getLast(DatabaseEntry key,
                                   DatabaseEntry pKey,
                                   DatabaseEntry data,
                                   LockMode lockMode)
        throws DatabaseException {

        OperationStatus status = OperationStatus.NOTFOUND;

        if (!range.hasBound()) {
            setParams(key, pKey, data);
            status = doGetLast(lockMode);
            endOperation(null, status, null, null, null);
            return status;
        }

        Cursor oldCursor = beginOperation();
        try {
            if (pkRange != null) {
                status = getLastInPKeyRange(lockMode);

                /* Final check on candidate key and pKey value. */
                if (status == OperationStatus.SUCCESS &&
                    !(range.check(privKey) && pkRange.check(privPKey))) {
                    status = OperationStatus.NOTFOUND;
                }
            } else {
                status = getLastInKeyRange(lockMode);

                /* Final check on candidate key value. */
                if (status == OperationStatus.SUCCESS &&
                    !range.check(privKey)) {
                    status = OperationStatus.NOTFOUND;
                }
            }

            return status;
        } finally {
            endOperation(oldCursor, status, key, pKey, data);
        }
    }

    /**
     * Performs getLast operation when a main key range is specified but
     * pkRange is null. Does everything but the final checks for key in range,
     * i.e., when SUCCESS is returned the caller should do the final check.
     */
    private OperationStatus getLastInKeyRange(LockMode lockMode)
        throws DatabaseException {

        /* Without an endKey, getLast returns the candidate record. */
        if (range.endKey == null) {
            return doGetLast(lockMode);
        }

        /*
         * K stands for the main key at the cursor position in the comments
         * below.
         */
        while (true) {
            KeyRange.copy(range.endKey, privKey);
            OperationStatus status = status = doGetSearchKeyRange(lockMode);

            if (status == OperationStatus.SUCCESS) {

                /* Found K >= endKey. */
                if (range.endInclusive &&
                    range.compare(range.endKey, privKey) == 0) {

                    /* K == endKey and endKey is inclusive. */

                    if (!sortedDups) {
                        /* If dups are not configured, we're done. */
                        return OperationStatus.SUCCESS;
                    }

                    /*
                     * If there are dups, we're positioned at endKey's first
                     * dup and we want to move to its last dup. Move to the
                     * first dup for the next main key (getNextNoDup) and then
                     * the prev record. In the absence of insertions by other
                     * threads, the prev record is the last dup for endKey.
                     */
                    status = doGetNextNoDup(lockMode);
                    if (status == OperationStatus.SUCCESS) {

                        /*
                         * K > endKey. Move backward to the last dup for
                         * endKey.
                         */
                        status = doGetPrev(lockMode);
                    } else {

                        /*
                         * endKey is the last main key in the DB. Its last dup
                         * is the last key in the DB.
                         */
                        status = doGetLast(lockMode);
                    }
                } else {

                    /*
                     * K > endKey or endKey is exclusive (and K >= endKey). In
                     * both cases, moving to the prev key finds the last key in
                     * the range, whether or not there are dups.
                     */
                    status = doGetPrev(lockMode);
                }
            } else {

                /*
                 * There are no keys >= endKey in the DB. The last key in the
                 * range is the last key in the DB.
                 */
                status = doGetLast(lockMode);
            }

            if (status != OperationStatus.SUCCESS) {
                return status;
            }

            if (!range.checkEnd(privKey, true)) {

                /*
                 * The last call above (getPrev or getLast) returned a key
                 * outside the endKey range. Another thread must have inserted
                 * this key. Start over.
                 */
                continue;
            }

            return status;
        }
    }

    /**
     * Performs getLast operation when both a main key range (which must be a
     * single key range) and a pkRange are specified. Does everything but the
     * final checks for key and pKey in range, i.e., when SUCCESS is returned
     * the caller should do the final two checks.
     */
    private OperationStatus getLastInPKeyRange(LockMode lockMode)
        throws DatabaseException {

        /* We can do an exact search when range and pkRange are single keys. */
        if (pkRange.isSingleKey()) {
            KeyRange.copy(range.beginKey, privKey);
            KeyRange.copy(pkRange.beginKey, privPKey);
            return doGetSearchBoth(lockMode);
        }

        /*
         * When dups are not configured, getSearchKey for the main key returns
         * the only possible candidate record.
         */
        if (!sortedDups) {
            KeyRange.copy(range.beginKey, privKey);
            return doGetSearchKey(lockMode);
        }

        /*
         * K stands for the main key and D for the duplicate (data item) at the
         * cursor position in the comments below
         */
        while (true) {

            if (pkRange.endKey != null) {

                KeyRange.copy(range.beginKey, privKey);
                KeyRange.copy(pkRange.endKey, privPKey);
                OperationStatus status = doGetSearchBothRange(lockMode);

                if (status == OperationStatus.SUCCESS) {

                    /* Found D >= endKey. */
                    if (!pkRange.endInclusive ||
                        pkRange.compare(pkRange.endKey, privPKey) != 0) {

                        /*
                         * D > endKey or endKey is exclusive (and D >= endKey).
                         * In both cases, moving to the prev dup finds the last
                         * key in the range.
                         */
                        status = doGetPrevDup(lockMode);

                        if (status != OperationStatus.SUCCESS) {
                            return status;
                        }

                        if (!pkRange.checkEnd(privPKey, true)) {

                            /*
                             * getPrevDup returned a key outside the endKey
                             * range. Another thread must have inserted this
                             * key. Start over.
                             */
                            continue;
                        }
                    }
                    /* Else D == endKey and endKey is inclusive. */

                    return OperationStatus.SUCCESS;
                }
                /* Else there are no dups >= endKey.  Fall through. */
            }

            /*
             * We're here for one of two reasons:
             *  1. pkRange.endKey == null.
             *  2. There are no dups >= endKey for the main key (status
             *     returned by getSearchBothRange above was not SUCCESS).
             * In both cases, the last dup in the range is the last dup for the
             * main key.
             */
            KeyRange.copy(range.beginKey, privKey);
            OperationStatus status = doGetSearchKey(lockMode);

            if (status != OperationStatus.SUCCESS) {
                return status;
            }

            /*
             * K == the main key and D is its first dup. We want to move to its
             * last dup. Move to the first dup for the next main key;
             * (getNextNoDup) and then the prev record. In the absence of
             * insertions by other threads, the prev record is the last dup for
             * the main key.
             */
            status = doGetNextNoDup(lockMode);

            if (status == OperationStatus.SUCCESS) {

                /*
                 * K > main key and D is its first dup. Move to the prev record
                 * which should be the last dup for the main key.
                 */
                status = doGetPrev(lockMode);
            } else {

                /*
                 * The main key specified is the last main key in the DB. Its
                 * last dup is the last record in the DB.
                 */
                status = doGetLast(lockMode);
            }

            if (status != OperationStatus.SUCCESS) {
                return status;
            }

            if (!range.checkEnd(privKey, true)) {

                /*
                 * The last call above (getPrev or getLast) returned a key
                 * outside the endKey range. Another thread must have inserted
                 * this key. Start over.
                 */
                continue;
            }

            return OperationStatus.SUCCESS;
        }
    }

    public OperationStatus getNext(DatabaseEntry key,
                                   DatabaseEntry pKey,
                                   DatabaseEntry data,
                                   LockMode lockMode)
        throws DatabaseException {

        OperationStatus status;
        if (!initialized) {
            return getFirst(key, pKey, data, lockMode);
        }
        if (!range.hasBound()) {
            setParams(key, pKey, data);
            status = doGetNext(lockMode);
            endOperation(null, status, null, null, null);
            return status;
        }
        if (pkRange != null) {
            if (pkRange.endKey == null) {
                status = doGetNextDup(lockMode);
                endOperation(null, status, key, pKey, data);
            } else {
                status = OperationStatus.NOTFOUND;
                Cursor oldCursor = beginOperation();
                try {
                    status = doGetNextDup(lockMode);
                    if (status == OperationStatus.SUCCESS &&
                        !pkRange.checkEnd(privPKey, true)) {
                        status = OperationStatus.NOTFOUND;
                    }
                } finally {
                    endOperation(oldCursor, status, key, pKey, data);
                }
            }
        } else if (range.singleKey) {
            status = doGetNextDup(lockMode);
            endOperation(null, status, key, pKey, data);
        } else {
            status = OperationStatus.NOTFOUND;
            Cursor oldCursor = beginOperation();
            try {
                status = doGetNext(lockMode);
                if (status == OperationStatus.SUCCESS &&
                    !range.check(privKey)) {
                    status = OperationStatus.NOTFOUND;
                }
            } finally {
                endOperation(oldCursor, status, key, pKey, data);
            }
        }
        return status;
    }

    public OperationStatus getNextNoDup(DatabaseEntry key,
                                        DatabaseEntry pKey,
                                        DatabaseEntry data,
                                        LockMode lockMode)
        throws DatabaseException {

        OperationStatus status;
        if (!initialized) {
            return getFirst(key, pKey, data, lockMode);
        }
        if (!range.hasBound()) {
            setParams(key, pKey, data);
            status = doGetNextNoDup(lockMode);
            endOperation(null, status, null, null, null);
            return status;
        }
        if (range.singleKey) {
            status = OperationStatus.NOTFOUND;
        } else {
            status = OperationStatus.NOTFOUND;
            Cursor oldCursor = beginOperation();
            try {
                status = doGetNextNoDup(lockMode);
                if (status == OperationStatus.SUCCESS &&
                    !range.check(privKey)) {
                    status = OperationStatus.NOTFOUND;
                }
            } finally {
                endOperation(oldCursor, status, key, pKey, data);
            }
        }
        return status;
    }

    public OperationStatus getPrev(DatabaseEntry key,
                                   DatabaseEntry pKey,
                                   DatabaseEntry data,
                                   LockMode lockMode)
        throws DatabaseException {

        OperationStatus status;
        if (!initialized) {
            return getLast(key, pKey, data, lockMode);
        }
        if (!range.hasBound()) {
            setParams(key, pKey, data);
            status = doGetPrev(lockMode);
            endOperation(null, status, null, null, null);
            return status;
        }
        if (pkRange != null) {
            if (pkRange.beginKey == null) {
                status = doGetPrevDup(lockMode);
                endOperation(null, status, key, pKey, data);
            } else {
                status = OperationStatus.NOTFOUND;
                Cursor oldCursor = beginOperation();
                try {
                    status = doGetPrevDup(lockMode);
                    if (status == OperationStatus.SUCCESS &&
                        !pkRange.checkBegin(privPKey, true)) {
                        status = OperationStatus.NOTFOUND;
                    }
                } finally {
                    endOperation(oldCursor, status, key, pKey, data);
                }
            }
        } else if (range.singleKey) {
            status = doGetPrevDup(lockMode);
            endOperation(null, status, key, pKey, data);
        } else {
            status = OperationStatus.NOTFOUND;
            Cursor oldCursor = beginOperation();
            try {
                status = doGetPrev(lockMode);
                if (status == OperationStatus.SUCCESS &&
                    !range.check(privKey)) {
                    status = OperationStatus.NOTFOUND;
                }
            } finally {
                endOperation(oldCursor, status, key, pKey, data);
            }
        }
        return status;
    }

    public OperationStatus getPrevNoDup(DatabaseEntry key,
                                        DatabaseEntry pKey,
                                        DatabaseEntry data,
                                        LockMode lockMode)
        throws DatabaseException {

        OperationStatus status;
        if (!initialized) {
            return getLast(key, pKey, data, lockMode);
        }
        if (!range.hasBound()) {
            setParams(key, pKey, data);
            status = doGetPrevNoDup(lockMode);
            endOperation(null, status, null, null, null);
            return status;
        }
        if (range.singleKey) {
            status = OperationStatus.NOTFOUND;
        } else {
            status = OperationStatus.NOTFOUND;
            Cursor oldCursor = beginOperation();
            try {
                status = doGetPrevNoDup(lockMode);
                if (status == OperationStatus.SUCCESS &&
                    !range.check(privKey)) {
                    status = OperationStatus.NOTFOUND;
                }
            } finally {
                endOperation(oldCursor, status, key, pKey, data);
            }
        }
        return status;
    }

    public OperationStatus getSearchKey(DatabaseEntry key,
                                        DatabaseEntry pKey,
                                        DatabaseEntry data,
                                        LockMode lockMode)
        throws DatabaseException {

        OperationStatus status;
        if (!range.hasBound()) {
            setParams(key, pKey, data);
            status = doGetSearchKey(lockMode);
            endOperation(null, status, null, null, null);
            return status;
        }
        if (!range.check(key)) {
            status = OperationStatus.NOTFOUND;
        } else if (pkRange != null) {
            status = OperationStatus.NOTFOUND;
            Cursor oldCursor = beginOperation();
            try {
                shareData(key, privKey);
                status = doGetSearchKey(lockMode);
                if (status == OperationStatus.SUCCESS &&
                    !pkRange.check(privPKey)) {
                    status = OperationStatus.NOTFOUND;
                }
            } finally {
                endOperation(oldCursor, status, key, pKey, data);
            }
        } else {
            shareData(key, privKey);
            status = doGetSearchKey(lockMode);
            endOperation(null, status, key, pKey, data);
        }
        return status;
    }

    public OperationStatus getSearchBoth(DatabaseEntry key,
                                         DatabaseEntry pKey,
                                         DatabaseEntry data,
                                         LockMode lockMode)
        throws DatabaseException {

        OperationStatus status;
        if (!range.hasBound()) {
            setParams(key, pKey, data);
            status = doGetSearchBoth(lockMode);
            endOperation(null, status, null, null, null);
            return status;
        }
        if (!range.check(key) ||
            (pkRange != null && !pkRange.check(pKey))) {
            status = OperationStatus.NOTFOUND;
        } else {
            shareData(key, privKey);
            if (secCursor != null) {
                shareData(pKey, privPKey);
            } else {
                shareData(data, privData);
            }
            status = doGetSearchBoth(lockMode);
            endOperation(null, status, key, pKey, data);
        }
        return status;
    }

    public OperationStatus getSearchKeyRange(DatabaseEntry key,
                                             DatabaseEntry pKey,
                                             DatabaseEntry data,
                                             LockMode lockMode)
        throws DatabaseException {

        OperationStatus status = OperationStatus.NOTFOUND;
        if (!range.hasBound()) {
            setParams(key, pKey, data);
            status = doGetSearchKeyRange(lockMode);
            endOperation(null, status, null, null, null);
            return status;
        }
        Cursor oldCursor = beginOperation();
        try {
            shareData(key, privKey);
            status = doGetSearchKeyRange(lockMode);
            if (status == OperationStatus.SUCCESS &&
                (!range.check(privKey) ||
                 (pkRange != null && !pkRange.check(pKey)))) {
                status = OperationStatus.NOTFOUND;
            }
        } finally {
            endOperation(oldCursor, status, key, pKey, data);
        }
        return status;
    }

    public OperationStatus getSearchBothRange(DatabaseEntry key,
                                              DatabaseEntry pKey,
                                              DatabaseEntry data,
                                              LockMode lockMode)
        throws DatabaseException {

        OperationStatus status = OperationStatus.NOTFOUND;
        if (!range.hasBound()) {
            setParams(key, pKey, data);
            status = doGetSearchBothRange(lockMode);
            endOperation(null, status, null, null, null);
            return status;
        }
        Cursor oldCursor = beginOperation();
        try {
            shareData(key, privKey);
            if (secCursor != null) {
                shareData(pKey, privPKey);
            } else {
                shareData(data, privData);
            }
            status = doGetSearchBothRange(lockMode);
            if (status == OperationStatus.SUCCESS &&
                (!range.check(privKey) ||
                 (pkRange != null && !pkRange.check(pKey)))) {
                status = OperationStatus.NOTFOUND;
            }
        } finally {
            endOperation(oldCursor, status, key, pKey, data);
        }
        return status;
    }

    public OperationStatus getSearchRecordNumber(DatabaseEntry key,
                                                 DatabaseEntry pKey,
                                                 DatabaseEntry data,
                                                 LockMode lockMode)
        throws DatabaseException {

        OperationStatus status;
        if (!range.hasBound()) {
            setParams(key, pKey, data);
            status = doGetSearchRecordNumber(lockMode);
            endOperation(null, status, null, null, null);
            return status;
        }
        if (!range.check(key)) {
            status = OperationStatus.NOTFOUND;
        } else {
            shareData(key, privKey);
            status = doGetSearchRecordNumber(lockMode);
            endOperation(null, status, key, pKey, data);
        }
        return status;
    }

    public OperationStatus getNextDup(DatabaseEntry key,
                                      DatabaseEntry pKey,
                                      DatabaseEntry data,
                                      LockMode lockMode)
        throws DatabaseException {

        if (!initialized) {
            throw new IllegalStateException("Cursor not initialized");
        }
        OperationStatus status;
        if (!range.hasBound()) {
            setParams(key, pKey, data);
            status = doGetNextDup(lockMode);
            endOperation(null, status, null, null, null);
        } else if (pkRange != null && pkRange.endKey != null) {
            status = OperationStatus.NOTFOUND;
            Cursor oldCursor = beginOperation();
            try {
                status = doGetNextDup(lockMode);
                if (status == OperationStatus.SUCCESS &&
                    !pkRange.checkEnd(privPKey, true)) {
                    status = OperationStatus.NOTFOUND;
                }
            } finally {
                endOperation(oldCursor, status, key, pKey, data);
            }
        } else {
            status = doGetNextDup(lockMode);
            endOperation(null, status, key, pKey, data);
        }
        return status;
    }

    public OperationStatus getPrevDup(DatabaseEntry key,
                                      DatabaseEntry pKey,
                                      DatabaseEntry data,
                                      LockMode lockMode)
        throws DatabaseException {

        if (!initialized) {
            throw new IllegalStateException("Cursor not initialized");
        }
        OperationStatus status;
        if (!range.hasBound()) {
            setParams(key, pKey, data);
            status = doGetPrevDup(lockMode);
            endOperation(null, status, null, null, null);
        } else if (pkRange != null && pkRange.beginKey != null) {
            status = OperationStatus.NOTFOUND;
            Cursor oldCursor = beginOperation();
            try {
                status = doGetPrevDup(lockMode);
                if (status == OperationStatus.SUCCESS &&
                    !pkRange.checkBegin(privPKey, true)) {
                    status = OperationStatus.NOTFOUND;
                }
            } finally {
                endOperation(oldCursor, status, key, pKey, data);
            }
        } else {
            status = doGetPrevDup(lockMode);
            endOperation(null, status, key, pKey, data);
        }
        return status;
    }

    public OperationStatus getCurrent(DatabaseEntry key,
                                      DatabaseEntry pKey,
                                      DatabaseEntry data,
                                      LockMode lockMode)
        throws DatabaseException {

        if (!initialized) {
            throw new IllegalStateException("Cursor not initialized");
        }
        if (secCursor != null && pKey != null) {
            return secCursor.getCurrent(key, pKey, data, lockMode);
        } else {
            return cursor.getCurrent(key, data, lockMode);
        }
    }

    /*
     * Pass-thru methods.
     */

    public void close()
        throws DatabaseException {

        closeCursor(cursor);
    }

    public int count()
        throws DatabaseException {

        return cursor.count();
    }

    public OperationStatus delete()
        throws DatabaseException {

        return cursor.delete();
    }

    public OperationStatus put(DatabaseEntry key, DatabaseEntry data)
        throws DatabaseException {

        return cursor.put(key, data);
    }

    public OperationStatus putNoOverwrite(DatabaseEntry key,
                                          DatabaseEntry data)
        throws DatabaseException {

        return cursor.putNoOverwrite(key, data);
    }

    public OperationStatus putNoDupData(DatabaseEntry key, DatabaseEntry data)
        throws DatabaseException {

        return cursor.putNoDupData(key, data);
    }

    public OperationStatus putCurrent(DatabaseEntry data)
        throws DatabaseException {

        return cursor.putCurrent(data);
    }

    public OperationStatus putAfter(DatabaseEntry key, DatabaseEntry data)
        throws DatabaseException {

        return DbCompat.putAfter(cursor, key, data);
    }

    public OperationStatus putBefore(DatabaseEntry key, DatabaseEntry data)
        throws DatabaseException {

        return DbCompat.putBefore(cursor, key, data);
    }

    private OperationStatus doGetFirst(LockMode lockMode)
        throws DatabaseException {

        if (secCursor != null && privPKey != null) {
            return secCursor.getFirst(privKey, privPKey, privData, lockMode);
        } else {
            return cursor.getFirst(privKey, privData, lockMode);
        }
    }

    private OperationStatus doGetLast(LockMode lockMode)
        throws DatabaseException {

        if (secCursor != null && privPKey != null) {
            return secCursor.getLast(privKey, privPKey, privData, lockMode);
        } else {
            return cursor.getLast(privKey, privData, lockMode);
        }
    }

    private OperationStatus doGetNext(LockMode lockMode)
        throws DatabaseException {

        if (secCursor != null && privPKey != null) {
            return secCursor.getNext(privKey, privPKey, privData, lockMode);
        } else {
            return cursor.getNext(privKey, privData, lockMode);
        }
    }

    private OperationStatus doGetNextDup(LockMode lockMode)
        throws DatabaseException {

        if (secCursor != null && privPKey != null) {
            return secCursor.getNextDup(privKey, privPKey, privData, lockMode);
        } else {
            return cursor.getNextDup(privKey, privData, lockMode);
        }
    }

    private OperationStatus doGetNextNoDup(LockMode lockMode)
        throws DatabaseException {

        if (secCursor != null && privPKey != null) {
            return secCursor.getNextNoDup(privKey, privPKey, privData,
                                          lockMode);
        } else {
            return cursor.getNextNoDup(privKey, privData, lockMode);
        }
    }

    private OperationStatus doGetPrev(LockMode lockMode)
        throws DatabaseException {

        if (secCursor != null && privPKey != null) {
            return secCursor.getPrev(privKey, privPKey, privData, lockMode);
        } else {
            return cursor.getPrev(privKey, privData, lockMode);
        }
    }

    private OperationStatus doGetPrevDup(LockMode lockMode)
        throws DatabaseException {

        if (secCursor != null && privPKey != null) {
            return secCursor.getPrevDup(privKey, privPKey, privData, lockMode);
        } else {
            return cursor.getPrevDup(privKey, privData, lockMode);
        }
    }

    private OperationStatus doGetPrevNoDup(LockMode lockMode)
        throws DatabaseException {

        if (secCursor != null && privPKey != null) {
            return secCursor.getPrevNoDup(privKey, privPKey, privData,
                                          lockMode);
        } else {
            return cursor.getPrevNoDup(privKey, privData, lockMode);
        }
    }

    private OperationStatus doGetSearchKey(LockMode lockMode)
        throws DatabaseException {

        if (checkRecordNumber() && DbCompat.getRecordNumber(privKey) <= 0) {
            return OperationStatus.NOTFOUND;
        }
        if (secCursor != null && privPKey != null) {
            return secCursor.getSearchKey(privKey, privPKey, privData,
                                          lockMode);
        } else {
            return cursor.getSearchKey(privKey, privData, lockMode);
        }
    }

    private OperationStatus doGetSearchKeyRange(LockMode lockMode)
        throws DatabaseException {

        if (checkRecordNumber() && DbCompat.getRecordNumber(privKey) <= 0) {
            return OperationStatus.NOTFOUND;
        }
        if (secCursor != null && privPKey != null) {
            return secCursor.getSearchKeyRange(privKey, privPKey, privData,
                                               lockMode);
        } else {
            return cursor.getSearchKeyRange(privKey, privData, lockMode);
        }
    }

    private OperationStatus doGetSearchBoth(LockMode lockMode)
        throws DatabaseException {

        if (checkRecordNumber() && DbCompat.getRecordNumber(privKey) <= 0) {
            return OperationStatus.NOTFOUND;
        }
        if (secCursor != null && privPKey != null) {
            return secCursor.getSearchBoth(privKey, privPKey, privData,
                                           lockMode);
        } else {
            return cursor.getSearchBoth(privKey, privData, lockMode);
        }
    }

    private OperationStatus doGetSearchBothRange(LockMode lockMode)
        throws DatabaseException {

        if (checkRecordNumber() && DbCompat.getRecordNumber(privKey) <= 0) {
            return OperationStatus.NOTFOUND;
        }
        if (secCursor != null && privPKey != null) {
            return secCursor.getSearchBothRange(privKey, privPKey,
                                                privData, lockMode);
        } else {
            return cursor.getSearchBothRange(privKey, privData, lockMode);
        }
    }

    private OperationStatus doGetSearchRecordNumber(LockMode lockMode)
        throws DatabaseException {

        if (DbCompat.getRecordNumber(privKey) <= 0) {
            return OperationStatus.NOTFOUND;
        }
        if (secCursor != null && privPKey != null) {
            return DbCompat.getSearchRecordNumber(secCursor, privKey, privPKey,
                                                  privData, lockMode);
        } else {
            return DbCompat.getSearchRecordNumber(cursor, privKey, privData,
                                                  lockMode);
        }
    }

    /*
     * Protected methods for duping and closing cursors.  These are overridden
     * by the collections API to implement cursor pooling for CDS.
     */

    /**
     * Dups the given cursor.
     */
    protected Cursor dupCursor(Cursor cursor, boolean samePosition)
        throws DatabaseException {

        return cursor.dup(samePosition);
    }

    /**
     * Closes the given cursor.
     */
    protected void closeCursor(Cursor cursor)
        throws DatabaseException {

        cursor.close();
    }

    /**
     * If the database is a RECNO or QUEUE database, we know its keys are
     * record numbers.  We treat a non-positive record number as out of bounds,
     * that is, we return NOTFOUND rather than throwing
     * IllegalArgumentException as would happen if we passed a non-positive
     * record number into the DB cursor.  This behavior is required by the
     * collections interface.
     */
    protected boolean checkRecordNumber() {
        return false;
    }
}

/*
 * Copyright (c) 2017-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.newapi;

import org.neo4j.graphdb.NotInTransactionException;
import org.neo4j.internal.kernel.api.CursorFactory;

public final class OpenCursorFactory {

    // The full-access wrapper for index cursors is, for some reason, not public.
    // This is allowing access to those cursor allocators for compat modules without requiring reflection.
    public static DefaultPooledCursors accessDefaultPooledCursors(CursorFactory cursors) {
        // There are three implementations of CursorFactory:
        //  - one is for the cypher runtime (CursorPools)
        //  - one is for usages outside of transactions (DefaultThreadSafeCursors)
        // The last one is the one we need and check for: DefaultPooledCursors.
        // If we got a CursorFactory by any other means than getting it from the KernelTransaction
        // this check will fail.
        if (cursors instanceof DefaultPooledCursors) {
            return (DefaultPooledCursors) cursors;
        }
        throw new NotInTransactionException("It looks like we are not in a transaction");
    }

    private OpenCursorFactory() {
        throw new UnsupportedOperationException("No instances");
    }
}

/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.gds.compat;

public interface StoreScan<Cursor extends org.neo4j.internal.kernel.api.Cursor> {

    /**
     * Advances the cursor to the next batch of the underlying scan.
     *
     * @param cursor a cursor to read the next batch
     * @param ctx    the execution context of the executing kernel transaction
     * @return true, iff the current batch contains data that must be consumed.
     */
    boolean reserveBatch(Cursor cursor, CompatExecutionContext ctx);
}

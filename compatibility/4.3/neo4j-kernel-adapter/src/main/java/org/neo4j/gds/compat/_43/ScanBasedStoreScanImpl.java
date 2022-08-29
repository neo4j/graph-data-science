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
package org.neo4j.gds.compat._43;

import org.neo4j.gds.compat.StoreScan;
import org.neo4j.internal.kernel.api.Cursor;
import org.neo4j.internal.kernel.api.Scan;
import org.neo4j.kernel.api.KernelTransaction;

final class ScanBasedStoreScanImpl<C extends Cursor> implements StoreScan<C> {
    private final Scan<C> scan;
    private final int batchSize;

    ScanBasedStoreScanImpl(Scan<C> scan, int batchSize) {
        this.scan = scan;
        this.batchSize = batchSize;
    }

    @Override
    public boolean reserveBatch(C cursor, KernelTransaction ktx) {
        return scan.reserveBatch(cursor, batchSize);
    }
}

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
package org.neo4j.graphalgo.core.loading;

import org.neo4j.graphalgo.compat.CompositeNodeCursor;
import org.neo4j.internal.kernel.api.NodeLabelIndexCursor;
import org.neo4j.internal.kernel.api.Scan;

import java.util.List;

public class CompositeNodeScan implements StoreScanner.StoreScan<CompositeNodeCursor> {

    private final List<Scan<NodeLabelIndexCursor>> scans;
    private final int batchSize;

    public CompositeNodeScan(List<Scan<NodeLabelIndexCursor>> scans, int batchSize) {
        this.scans = scans;
        this.batchSize = batchSize;
    }

    // This method needs to be synchronized as we need to make sure that every subscan is processing the same batch
    @Override
    public synchronized boolean scanBatch(CompositeNodeCursor cursor) {
        boolean result = false;
        for (int i = 0; i < scans.size(); i++) {
            NodeLabelIndexCursor indexCursor = cursor.getCursor(i);
            if (indexCursor != null) {
                var hasNextBatch = scans.get(i).reserveBatch(indexCursor, batchSize);
                if (hasNextBatch) {
                    result = true;
                } else {
                    cursor.removeCursor(i);
                }
            }
        }

        return result;
    }
}

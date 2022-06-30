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
package org.neo4j.gds.ml.core.batch;

import java.util.PrimitiveIterator;

public class RangeBatch implements Batch {
    private final long startId;
    private final long endId;
    private final int size;

    public RangeBatch(long startId, int batchSize, long nodeCount) {
        this.startId = startId;
        this.endId = Math.min(nodeCount, startId + batchSize);
        this.size = (int) (this.endId - this.startId);
    }

    @Override
    public PrimitiveIterator.OfLong elementIds() {
        return new PrimitiveIterator.OfLong() {

            int offset = 0;
            @Override
            public long nextLong() {
                return startId + offset++;
            }

            @Override
            public boolean hasNext() {
                return startId + offset < endId;
            }
        };
    }

    @Override
    public int size() {
        return size;
    }
}

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

import java.util.Optional;

class ConsecutiveBatchQueue extends BatchQueue {

    ConsecutiveBatchQueue(long totalSize, int batchSize) {
        super(totalSize, batchSize);
    }

    @Override
    synchronized Optional<Batch> pop() {
        if (currentBatch * batchSize >= totalSize) {
            return Optional.empty();
        }
        var batch = new RangeBatch(currentBatch * batchSize, batchSize, totalSize);
        currentBatch += 1;
        return Optional.of(batch);
    }
}

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

import org.neo4j.gds.core.utils.paged.ReadOnlyHugeLongArray;

import java.util.LinkedList;
import java.util.Optional;

public class HugeBatchQueue extends BatchQueue {
    private final ReadOnlyHugeLongArray data;

    public HugeBatchQueue(ReadOnlyHugeLongArray data) {
        super(data.size(), DEFAULT_BATCH_SIZE);
        this.data = data;
    }

    public HugeBatchQueue(ReadOnlyHugeLongArray data, int batchSize) {
        super(data.size(), batchSize);
        this.data = data;
    }

    @Override
    public synchronized Optional<Batch> pop() {
        if (currentBatch * batchSize >= data.size()) {
            return Optional.empty();
        }
        var batchIds = new LinkedList<Long>();
        for (long offset = currentBatch * batchSize; offset < data.size() && offset < (currentBatch + 1) * batchSize; offset++) {
            batchIds.add(data.get(offset));
        }
        Batch batch = new ListBatch(batchIds);
        currentBatch += 1;
        return Optional.of(batch);
    }
}

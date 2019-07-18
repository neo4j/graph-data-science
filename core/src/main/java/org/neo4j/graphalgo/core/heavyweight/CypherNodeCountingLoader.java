/*
 * Copyright (c) 2017-2019 "Neo4j,"
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
package org.neo4j.graphalgo.core.heavyweight;

import org.neo4j.graphalgo.api.GraphSetup;
import org.neo4j.graphalgo.core.GraphDimensions;
import org.neo4j.graphalgo.core.IntIdMap;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import static org.neo4j.graphalgo.core.heavyweight.HeavyCypherGraphFactory.INITIAL_NODE_COUNT;
import static org.neo4j.graphalgo.core.heavyweight.HeavyCypherGraphFactory.NO_BATCH;

class CypherNodeCountingLoader {
    private final GraphDatabaseAPI api;
    private final GraphSetup setup;
    private final GraphDimensions dimensions;

    public CypherNodeCountingLoader(GraphDatabaseAPI api, GraphSetup setup, GraphDimensions dimensions) {
        this.api = api;
        this.setup = setup;
        this.dimensions = dimensions;
    }

    public NodeCount load() {
        int batchSize = setup.batchSize;
        return CypherLoadingUtils.canBatchLoad(setup.loadConcurrent(), batchSize, setup.startLabel) ?
                batchLoadNodes(batchSize) :
                loadNodes(0, NO_BATCH);
    }

    private NodeCount batchLoadNodes(int batchSize) {
        ExecutorService pool = setup.executor;
        int threads = setup.concurrency();

        long offset = 0;
        long total = 0;
        long lastOffset = 0;
        List<Future<NodeCount>> futures = new ArrayList<>(threads);
        boolean working = true;
        do {
            long skip = offset;
            futures.add(pool.submit(() -> loadNodes(skip, batchSize)));
            offset += batchSize;
            if (futures.size() >= threads) {
                for (Future<NodeCount> future : futures) {
                    NodeCount result = CypherLoadingUtils.get("Error during loading nodes offset: " + (lastOffset + batchSize), future);
                    lastOffset = result.offset();
                    total += result.rows();
                    working = result.rows > 0;
                }
                futures.clear();
            }
        } while (working);

        return new NodeCount(total, 0);
    }

    private NodeCount loadNodes(long offset, int batchSize) {
        int capacity = batchSize == NO_BATCH ? INITIAL_NODE_COUNT : batchSize;
        final IntIdMap idMap = new IntIdMap(capacity);

        NodeRowCountingVisitor visitor = new NodeRowCountingVisitor();
        api.execute(setup.startLabel, CypherLoadingUtils.params(setup.params,offset, batchSize)).accept(visitor);
        idMap.buildMappedIds(setup.tracker);
        return new NodeCount(visitor.rows(), offset);
    }

    static class NodeCount {

        private final long rows;
        private final long offset;

        NodeCount(final long rows, final long offset) {

            this.rows = rows;
            this.offset = offset;
        }

        long offset() {
            return offset;
        }

        long rows() {
            return rows;
        }
    }
}

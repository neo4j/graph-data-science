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
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import static org.neo4j.graphalgo.core.heavyweight.HeavyCypherGraphFactory.NO_BATCH;

class CypherNodeCountingLoader {
    private final GraphDatabaseAPI api;
    private final GraphSetup setup;

    CypherNodeCountingLoader(GraphDatabaseAPI api, GraphSetup setup) {
        this.api = api;
        this.setup = setup;
    }

    public ImportState load() {
        int batchSize = setup.batchSize;
        return CypherLoadingUtils.canBatchLoad(setup.loadConcurrent(), batchSize, setup.startLabel) ?
                parallelLoadNodes(batchSize) :
                nonParallelLoadNodes(0, NO_BATCH, false);
    }

    private ImportState parallelLoadNodes(int batchSize) {
        ExecutorService pool = setup.executor;
        int threads = setup.concurrency();

        long offset = 0;
        long total = 0;
        long lastOffset = 0;
        List<Future<ImportState>> futures = new ArrayList<>(threads);
        boolean working = true;
        do {
            long skip = offset;
            futures.add(pool.submit(() -> nonParallelLoadNodes(skip, batchSize, true)));
            offset += batchSize;
            if (futures.size() >= threads) {
                for (Future<ImportState> future : futures) {
                    ImportState result = CypherLoadingUtils.get("Error during loading nodes offset: " + (lastOffset + batchSize), future);
                    lastOffset = result.offset();
                    total += result.rows();
                    working = result.rows() > 0;
                }
                futures.clear();
            }
        } while (working);

        return new ImportState(0, total, -1L, -1L);
    }

    private ImportState nonParallelLoadNodes(final long offset, final int batchSize, boolean withPaging) {
        ResultCountingVisitor visitor = new ResultCountingVisitor();
        Map<String, Object> parameters = withPaging
                ? CypherLoadingUtils.params(setup.params, offset, batchSize)
                : setup.params;
        api.execute(setup.startLabel, parameters).accept(visitor);
        return new ImportState(offset, visitor.rows(), -1L, -1L);
    }

}

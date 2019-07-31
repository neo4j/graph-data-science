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
import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import static org.neo4j.graphalgo.core.heavyweight.HeavyCypherGraphFactory.NO_BATCH;

public class CypherRelationshipCountingLoader {
    private final GraphDatabaseAPI api;
    private final GraphSetup setup;

    CypherRelationshipCountingLoader(final GraphDatabaseAPI api, final GraphSetup setup) {
        this.api = api;
        this.setup = setup;
    }

    public RelationshipCount load() {
        int batchSize = setup.batchSize;
        return CypherLoadingUtils.canBatchLoad(setup.loadConcurrent(), batchSize, setup.relationshipType) ?
                batchLoadRelationships(batchSize) :
                loadRelationships(0, NO_BATCH);
    }

    private RelationshipCount batchLoadRelationships(int batchSize) {
        ExecutorService pool = setup.executor;
        int threads = setup.concurrency();

        long offset = 0;
        long lastOffset = 0;
        long total = 0;
        Collection<Future<RelationshipCount>> futures = new ArrayList<>(threads);
        boolean working = true;
        do {
            long skip = offset;
            futures.add(pool.submit(() -> loadRelationships(skip, batchSize)));
            offset += batchSize;
            if (futures.size() >= threads) {
                for (Future<RelationshipCount> future : futures) {
                    RelationshipCount result = CypherLoadingUtils.get(
                            "Error during loading relationships offset: " + (lastOffset + batchSize),
                            future);
                    lastOffset = result.offset();
                    total += result.rows();
                    working = result.rows() > 0;
                }
                futures.clear();
            }
        } while (working);

        return new RelationshipCount(total, 0);
    }

    private RelationshipCount loadRelationships(long offset, int batchSize) {
        ResultCountingVisitor visitor = new ResultCountingVisitor();
        api.execute(setup.relationshipType, CypherLoadingUtils.params(setup.params, offset, batchSize)).accept(visitor);
        return new RelationshipCount(visitor.rows(), offset);
    }

    static class RelationshipCount {

        private final long rows;
        private final long offset;

        RelationshipCount(final long rows, final long offset) {

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

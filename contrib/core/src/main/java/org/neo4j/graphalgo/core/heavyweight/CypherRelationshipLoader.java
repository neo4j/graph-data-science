/*
 * Copyright (c) 2017 "Neo4j, Inc." <http://neo4j.com>
 *
 * This file is part of Neo4j Graph Algorithms <http://github.com/neo4j-contrib/neo4j-graph-algorithms>.
 *
 * Neo4j Graph Algorithms is free software: you can redistribute it and/or modify
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
import org.neo4j.graphalgo.core.IdMap;
import org.neo4j.graphalgo.core.WeightMap;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import static org.neo4j.graphalgo.core.heavyweight.CypherLoadingUtils.newWeightMapping;
import static org.neo4j.graphalgo.core.heavyweight.HeavyCypherGraphFactory.NO_BATCH;

public class CypherRelationshipLoader {
    private final GraphDatabaseAPI api;
    private final GraphSetup setup;

    public CypherRelationshipLoader(GraphDatabaseAPI api, GraphSetup setup) {
        this.api = api;
        this.setup = setup;
    }

    public Relationships load(Nodes nodes) {
        int batchSize = setup.batchSize;
        return CypherLoadingUtils.canBatchLoad(setup.loadConcurrent(), batchSize, setup.relationshipType) ?
                batchLoadRelationships(batchSize, nodes) :
                loadRelationships(0, NO_BATCH, nodes);
    }

    private Relationships batchLoadRelationships(int batchSize, Nodes nodes) {
        ExecutorService pool = setup.executor;
        int threads = setup.concurrency();
        int nodeCount = nodes.idMap.size();

        MergedRelationships mergedRelationships = new MergedRelationships(nodeCount, setup, setup.duplicateRelationshipsStrategy);

        long offset = 0;
        long lastOffset = 0;
        long total = 0;
        List<Future<Relationships>> futures = new ArrayList<>(threads);
        boolean working = true;
        do {
            long skip = offset;
            // suboptimal, each sub-call allocates a AdjacencyMatrix of nodeCount size, would be better with a sparse variant
            futures.add(pool.submit(() -> loadRelationships(skip, batchSize, nodes)));
            offset += batchSize;
            if (futures.size() >= threads) {
                for (Future<Relationships> future : futures) {
                    Relationships result = CypherLoadingUtils.get(
                            "Error during loading relationships offset: " + (lastOffset + batchSize),
                            future);
                    lastOffset = result.offset();
                    total += result.rows();
                    working = mergedRelationships.canMerge(result);
                    if (working) {
                        mergedRelationships.merge(result);
                    }
                }
                futures.clear();
            }
        } while (working);

        return new Relationships(0, total, mergedRelationships.matrix(), mergedRelationships.relWeights(), setup.relationDefaultWeight);
    }

    private Relationships loadRelationships(long offset, int batchSize, Nodes nodes) {

        IdMap idMap = nodes.idMap;

        int nodeCount = idMap.size();
        int capacity = batchSize == NO_BATCH ? nodeCount : batchSize;

        final AdjacencyMatrix matrix = new AdjacencyMatrix(nodeCount, false, setup.tracker);

        boolean hasRelationshipWeights = setup.shouldLoadRelationshipWeight();
        final WeightMap relWeights = newWeightMapping(hasRelationshipWeights, setup.relationDefaultWeight, capacity);

        RelationshipRowVisitor visitor = new RelationshipRowVisitor(idMap, hasRelationshipWeights, relWeights, matrix, setup.duplicateRelationshipsStrategy);
        api.execute(setup.relationshipType, CypherLoadingUtils.params(setup.params, offset, batchSize)).accept(visitor);
        return new Relationships(offset, visitor.rows(), matrix, relWeights, setup.relationDefaultWeight);
    }


}

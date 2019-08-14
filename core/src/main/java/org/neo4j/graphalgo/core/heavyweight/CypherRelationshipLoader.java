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
import org.neo4j.graphalgo.core.huge.HugeAdjacencyList;
import org.neo4j.graphalgo.core.huge.HugeAdjacencyOffsets;
import org.neo4j.graphalgo.core.huge.loader.AdjacencyBuilder;
import org.neo4j.graphalgo.core.huge.loader.IdsAndProperties;
import org.neo4j.graphalgo.core.huge.loader.ImportSizing;
import org.neo4j.graphalgo.core.huge.loader.RelationshipImporter;
import org.neo4j.graphalgo.core.huge.loader.RelationshipImporter.Imports;
import org.neo4j.graphalgo.core.huge.loader.RelationshipsBatchBuffer;
import org.neo4j.graphalgo.core.huge.loader.RelationshipsBuilder;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.kernel.api.StatementConstants;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

public class CypherRelationshipLoader {
    private final GraphDatabaseAPI api;
    private final GraphSetup setup;
    private final RelationshipsBuilder outgoingRelationshipsBuilder;
    private long totalRecordsSeen = 0;
    private long totalRelationshipsImported = 0;


    public CypherRelationshipLoader(GraphDatabaseAPI api, GraphSetup setup) {
        this.api = api;
        this.setup = setup;
        outgoingRelationshipsBuilder = new RelationshipsBuilder(setup.duplicateRelationshipsStrategy, setup.tracker, setup.shouldLoadRelationshipWeight());
    }

    public Relationships load(IdsAndProperties nodes) {
        int batchSize = setup.batchSize;
        ImportSizing importSizing = ImportSizing.of(setup.concurrency, nodes.idMap().nodeCount());
        int pageSize = importSizing.pageSize();
        int numberOfPages = importSizing.numberOfPages();

        AdjacencyBuilder outBuilder = AdjacencyBuilder.compressing(
                outgoingRelationshipsBuilder,
                numberOfPages, pageSize,
                setup.tracker, new AtomicLong(), -2, setup.relationDefaultWeight);

        boolean hasRelationshipWeights = setup.shouldLoadRelationshipWeight();

        org.neo4j.graphalgo.core.huge.loader.RelationshipImporter importer = new org.neo4j.graphalgo.core.huge.loader.RelationshipImporter(setup.tracker, outBuilder, null);

        Imports imports = RelationshipImporter.imports(importer, false, true, false, hasRelationshipWeights);

        if (CypherLoadingUtils.canBatchLoad(setup.loadConcurrent(), batchSize, setup.relationshipType)) {
            parallelLoadRelationships(batchSize, nodes, importer, imports);
        } else {
            nonParallelLoadRelationships(nodes, importer, imports);
        }

        HugeAdjacencyList outAdjacencyList = outgoingRelationshipsBuilder.adjacency.build();
        HugeAdjacencyOffsets outAdjacencyOffsets = outgoingRelationshipsBuilder.globalAdjacencyOffsets;
        HugeAdjacencyList outWeightList = setup.shouldLoadRelationshipWeight() ? outgoingRelationshipsBuilder.weights.build() : null;
        HugeAdjacencyOffsets outWeightOffsets = setup.shouldLoadRelationshipWeight() ? outgoingRelationshipsBuilder.globalWeightOffsets : null;

        return new Relationships(
                totalRecordsSeen, totalRelationshipsImported,
                null,
                outAdjacencyList,
                null,
                outAdjacencyOffsets,
                setup.relationDefaultWeight,
                null,
                outWeightList,
                null,
                outWeightOffsets
        );

//        return new Relationships(
//                // TODO: rows, relationshipCount -- what is the difference?
//                //       deduplication, plus rels filtered because source or target is unknown
//                totalRecordsSeen, totalRelationshipsImported,
//                incomingRelationshipsBuilder.adjacency.build(),
//                outgoingRelationshipsBuilder.adjacency.build(),
//                incomingRelationshipsBuilder.globalAdjacencyOffsets,
//                outgoingRelationshipsBuilder.globalAdjacencyOffsets,
//                setup.relationDefaultWeight,
//                incomingRelationshipsBuilder.weights.build(),
//                outgoingRelationshipsBuilder.weights.build(),
//                incomingRelationshipsBuilder.globalWeightOffsets,
//                outgoingRelationshipsBuilder.globalWeightOffsets
//        );

    }

    private void parallelLoadRelationships(
            int batchSize,
            IdsAndProperties nodes,
            org.neo4j.graphalgo.core.huge.loader.RelationshipImporter importer,
            Imports imports) {
        ExecutorService pool = setup.executor;
        int threads = setup.concurrency();

        long offset = 0;
        long lastOffset = 0;
        List<Future<ImportState>> futures = new ArrayList<>(threads);
        boolean working = true;
        do {
            long skip = offset;
            // suboptimal, each sub-call allocates a AdjacencyMatrix of nodeCount size, would be better with a sparse variant
            futures.add(pool.submit(() -> loadRelationships(skip, batchSize, nodes, importer, imports, true)));
            offset += batchSize;
            if (futures.size() >= threads) {
                for (Future<ImportState> future : futures) {
                    ImportState result = CypherLoadingUtils.get("Error during loading relationships offset: " + (lastOffset + batchSize), future);
                    if (0 >= result.rows()) {
                        working = false;
                    }
                    lastOffset = result.offset();
                    totalRecordsSeen += result.rows();
                    totalRelationshipsImported += result.count();
                }
                futures.clear();
            }
        } while (working);

        ParallelUtil.run(importer.flushTasks(), pool);
    }

    private void nonParallelLoadRelationships(
            IdsAndProperties nodes,
            org.neo4j.graphalgo.core.huge.loader.RelationshipImporter importer,
            Imports imports) {
        ImportState relationships = loadRelationships(0L, ParallelUtil.DEFAULT_BATCH_SIZE, nodes, importer, imports, false);
        importer.flushTasks().forEach(Runnable::run);
        totalRecordsSeen = relationships.rows();
        totalRelationshipsImported = relationships.count();
    }

    private ImportState loadRelationships(
            long offset,
            int batchSize,
            IdsAndProperties nodes,
            org.neo4j.graphalgo.core.huge.loader.RelationshipImporter importer,
            Imports imports,
            boolean withPaging) {
        boolean hasRelationshipWeights = setup.shouldLoadRelationshipWeight();
        RelationshipsBatchBuffer buffer = new RelationshipsBatchBuffer(nodes.idMap(), StatementConstants.ANY_RELATIONSHIP_TYPE, batchSize);
        RelationshipRowVisitor visitor = new RelationshipRowVisitor(
                buffer,
                nodes.idMap(),
                hasRelationshipWeights,
                setup.relationDefaultWeight,
                importer,
                imports
        );
        Map<String, Object> parameters = withPaging
                ? CypherLoadingUtils.params(setup.params, offset, batchSize)
                : setup.params;
        api.execute(setup.relationshipType, parameters).accept(visitor);
        visitor.flush();

        return new ImportState(offset, visitor.rows(), -1L, visitor.relationshipCount());
    }

}

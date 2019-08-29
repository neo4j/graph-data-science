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
package org.neo4j.graphalgo.core.huge.loader;

import org.neo4j.graphalgo.api.GraphSetup;
import org.neo4j.graphalgo.core.DeduplicateRelationshipsStrategy;
import org.neo4j.graphalgo.core.huge.HugeAdjacencyList;
import org.neo4j.graphalgo.core.huge.HugeAdjacencyOffsets;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.kernel.api.StatementConstants;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.concurrent.atomic.AtomicLong;

class CypherRelationshipLoader extends CypherRecordLoader<Relationships> {

    private final IdMap idMap;
    private final RelationshipsBuilder outgoingRelationshipsBuilder;
    private final RelationshipImporter importer;
    private final RelationshipImporter.Imports imports;
    private final boolean hasRelationshipWeights;
    private final double relationDefaultWeight;

    private long totalRecordsSeen;
    private long totalRelationshipsImported;

    CypherRelationshipLoader(IdMap idMap, GraphDatabaseAPI api, GraphSetup setup) {
        super(setup.relationshipType, idMap.nodeCount(), api, setup);

        DeduplicateRelationshipsStrategy deduplicateRelationshipsStrategy =
                setup.deduplicateRelationshipsStrategy == DeduplicateRelationshipsStrategy.DEFAULT
                        ? DeduplicateRelationshipsStrategy.NONE
                        : setup.deduplicateRelationshipsStrategy;
        outgoingRelationshipsBuilder = new RelationshipsBuilder(
                deduplicateRelationshipsStrategy,
                setup.tracker,
                setup.shouldLoadRelationshipWeight());

        ImportSizing importSizing = ImportSizing.of(setup.concurrency, idMap.nodeCount());
        int pageSize = importSizing.pageSize();
        int numberOfPages = importSizing.numberOfPages();

        relationDefaultWeight = setup.relationDefaultWeight;
        AdjacencyBuilder outBuilder = AdjacencyBuilder.compressing(
                outgoingRelationshipsBuilder,
                numberOfPages, pageSize,
                setup.tracker, new AtomicLong(), -2, relationDefaultWeight);

        this.idMap = idMap;
        hasRelationshipWeights = setup.shouldLoadRelationshipWeight();
        importer = new RelationshipImporter(setup.tracker, outBuilder, null);
        imports = importer.imports(false, true, false, hasRelationshipWeights);
        totalRecordsSeen = 0;
        totalRelationshipsImported = 0;
    }

    @Override
    BatchLoadResult loadOneBatch(long offset, int batchSize, int bufferSize) {
        RelationshipsBatchBuffer buffer = new RelationshipsBatchBuffer(
                idMap,
                StatementConstants.ANY_RELATIONSHIP_TYPE,
                bufferSize);
        RelationshipRowVisitor visitor = new RelationshipRowVisitor(
                buffer,
                idMap,
                hasRelationshipWeights,
                relationDefaultWeight,
                importer,
                imports
        );
        runLoadingQuery(offset, batchSize, visitor);
        visitor.flush();
        return new BatchLoadResult(offset, visitor.rows(), -1L, visitor.relationshipCount());
    }

    @Override
    void updateCounts(BatchLoadResult result) {
        totalRecordsSeen += result.rows();
        totalRelationshipsImported += result.count();
    }

    @Override
    Relationships result() {
        ParallelUtil.run(importer.flushTasks(), setup.executor);

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
    }
}

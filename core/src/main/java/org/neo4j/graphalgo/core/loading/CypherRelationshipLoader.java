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
package org.neo4j.graphalgo.core.loading;

import org.neo4j.graphalgo.PropertyMapping;
import org.neo4j.graphalgo.api.GraphSetup;
import org.neo4j.graphalgo.core.DeduplicationStrategy;
import org.neo4j.graphalgo.core.huge.AdjacencyList;
import org.neo4j.graphalgo.core.huge.AdjacencyOffsets;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.kernel.api.StatementConstants;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.Optional;
import java.util.concurrent.atomic.LongAdder;

class CypherRelationshipLoader extends CypherRecordLoader<Relationships> {

    private static final int SINGLE_RELATIONSHIP_WEIGHT = 1;
    private static final int NO_RELATIONSHIP_WEIGHT = 0;
    private static final int DEFAULT_WEIGHT_PROPERTY_ID = -2;

    private final IdMap idMap;
    private final RelationshipsBuilder outgoingRelationshipsBuilder;
    private final RelationshipImporter importer;
    private final RelationshipImporter.Imports imports;
    private final Optional<Double> maybeDefaultRelProperty;

    private long totalRecordsSeen;
    private long totalRelationshipsImported;

    CypherRelationshipLoader(IdMap idMap, GraphDatabaseAPI api, GraphSetup setup) {
        super(setup.relationshipType(), idMap.nodeCount(), api, setup);

        DeduplicationStrategy deduplicationStrategy =
            setup.deduplicationStrategy() == DeduplicationStrategy.DEFAULT
                        ? DeduplicationStrategy.NONE
                        : setup.deduplicationStrategy();
        outgoingRelationshipsBuilder = new RelationshipsBuilder(
                new DeduplicationStrategy[]{deduplicationStrategy},
            setup.tracker(),
                setup.shouldLoadRelationshipProperties() ? SINGLE_RELATIONSHIP_WEIGHT : NO_RELATIONSHIP_WEIGHT);

        ImportSizing importSizing = ImportSizing.of(setup.concurrency(), idMap.nodeCount());
        int pageSize = importSizing.pageSize();
        int numberOfPages = importSizing.numberOfPages();

        this.maybeDefaultRelProperty = setup.relationshipDefaultPropertyValue();
        Double defaultRelationshipProperty = maybeDefaultRelProperty.orElseGet(PropertyMapping.EMPTY_PROPERTY::defaultValue);

        AdjacencyBuilder outBuilder = AdjacencyBuilder.compressing(
                outgoingRelationshipsBuilder,
                numberOfPages,
                pageSize,
            setup.tracker(),
                new LongAdder(),
                new int[]{DEFAULT_WEIGHT_PROPERTY_ID},
                new double[]{defaultRelationshipProperty}
        );

        this.idMap = idMap;
        importer = new RelationshipImporter(setup.tracker(), outBuilder, null);
        imports = importer.imports(false, true, false, maybeDefaultRelProperty.isPresent());
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
                maybeDefaultRelProperty,
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
        ParallelUtil.run(importer.flushTasks(), setup.executor());

        AdjacencyList outAdjacencyList = outgoingRelationshipsBuilder.adjacency.build();
        AdjacencyOffsets outAdjacencyOffsets = outgoingRelationshipsBuilder.globalAdjacencyOffsets;
        AdjacencyList outWeightList = setup.shouldLoadRelationshipProperties() ? outgoingRelationshipsBuilder.weights[0].build() : null;
        AdjacencyOffsets outWeightOffsets = setup.shouldLoadRelationshipProperties() ? outgoingRelationshipsBuilder.globalWeightOffsets[0] : null;

        return new Relationships(
                totalRecordsSeen, totalRelationshipsImported,
                null,
                outAdjacencyList,
                null,
                outAdjacencyOffsets,
            setup.relationshipDefaultPropertyValue(),
                null,
                outWeightList,
                null,
                outWeightOffsets
        );
    }
}

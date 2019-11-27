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

import org.apache.commons.lang3.tuple.Pair;
import org.neo4j.graphalgo.PropertyMapping;
import org.neo4j.graphalgo.RelationshipTypeMapping;
import org.neo4j.graphalgo.api.GraphSetup;
import org.neo4j.graphalgo.core.GraphDimensions;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toMap;

class CypherMultiRelationshipLoader extends CypherRecordLoader<Relationships> {

    private static final int SINGLE_RELATIONSHIP_WEIGHT = 1;
    private static final int NO_RELATIONSHIP_WEIGHT = 0;
    private static final int DEFAULT_WEIGHT_PROPERTY_ID = -2;

    private final IdMap idMap;
    private final Map<RelationshipTypeMapping, Pair<RelationshipsBuilder, RelationshipsBuilder>> allBuilders;
    private final GraphDimensions dimensions;
    private final int relationshipPropertyCount;

    private long totalRecordsSeen = 0L;
    private long totalRelationshipsImported = 0L;
    private final Map<RelationshipTypeMapping, SingleTypeRelationshipImporter.Builder.WithImporter> relationshipImporterBuilders;
    private final Map<String, Integer> propertyKeyIdsByName;
    private final Map<String, Double> propertyDefaultValueByName;

    CypherMultiRelationshipLoader(
        IdMap idMap,
        Map<RelationshipTypeMapping, Pair<RelationshipsBuilder, RelationshipsBuilder>> allBuilders,
        GraphDatabaseAPI api,
        GraphSetup setup,
        GraphDimensions dimensions
    ) {
        super(setup.relationshipType, idMap.nodeCount(), api, setup);
        this.idMap = idMap;

        this.allBuilders = allBuilders;
        this.dimensions = dimensions;

        ImportSizing importSizing = ImportSizing.of(setup.concurrency, idMap.nodeCount());
        int pageSize = importSizing.pageSize();
        int numberOfPages = importSizing.numberOfPages();

        boolean importWeights = dimensions.relProperties().atLeastOneExists();

        propertyKeyIdsByName = dimensions
            .relProperties()
            .stream()
            .collect(toMap(PropertyMapping::neoPropertyKey, PropertyMapping::propertyKeyId));

        propertyDefaultValueByName = dimensions
            .relProperties()
            .stream()
            .collect(toMap(PropertyMapping::neoPropertyKey, PropertyMapping::defaultValue));

        relationshipImporterBuilders = allBuilders
            .entrySet()
            .stream()
            .collect(toMap(
                Map.Entry::getKey,
                entry ->
                    createImporterBuilder(pageSize, numberOfPages, entry, setup.tracker)
                        .loadImporter(false, true, false, importWeights)
            ));

        relationshipPropertyCount = dimensions.relProperties().numberOfMappings();
    }

    @Override
    BatchLoadResult loadOneBatch(long offset, int batchSize, int bufferSize) {
        // TODO: think about intializing the buffers once within the constructor
        List<MultiRelationshipRowVisitor.Context> contexts = relationshipImporterBuilders
            .entrySet()
            .stream()
            .map(entry -> {
                    RelationshipTypeMapping relationshipTypeMapping = entry.getKey();
                    SingleTypeRelationshipImporter.Builder.WithImporter builder = entry.getValue();
                    RelationshipPropertyBatchBuffer propertyReader = new RelationshipPropertyBatchBuffer(
                        batchSize,
                        relationshipPropertyCount
                    );
                    SingleTypeRelationshipImporter importer = builder.withBuffer(idMap, bufferSize, propertyReader);
                    return new MultiRelationshipRowVisitor.Context(relationshipTypeMapping, importer, propertyReader);
                }
            ).collect(Collectors.toList());

        MultiRelationshipRowVisitor visitor = new MultiRelationshipRowVisitor(
            idMap,
            contexts,
            propertyKeyIdsByName,
            propertyDefaultValueByName
        );

        runLoadingQuery(offset, batchSize, visitor);
        visitor.flushAll();
        return new BatchLoadResult(offset, visitor.rows(), -1L, visitor.relationshipCount());
    }

    @Override
    void updateCounts(BatchLoadResult result) {
        totalRecordsSeen += result.rows();
        totalRelationshipsImported += result.count();
    }

    @Override
    Relationships result() {
        List<Runnable> flushTasks = relationshipImporterBuilders
            .values()
            .stream()
            .flatMap(SingleTypeRelationshipImporter.Builder.WithImporter::flushTasks)
            .collect(Collectors.toList());

        ParallelUtil.run(flushTasks, setup.executor);

        return null;
    }

    private SingleTypeRelationshipImporter.Builder createImporterBuilder(
        int pageSize,
        int numberOfPages,
        Map.Entry<RelationshipTypeMapping, Pair<RelationshipsBuilder, RelationshipsBuilder>> entry,
        AllocationTracker tracker
    ) {
        RelationshipTypeMapping mapping = entry.getKey();
        RelationshipsBuilder outRelationshipsBuilder = entry.getValue().getLeft();
        RelationshipsBuilder inRelationshipsBuilder = entry.getValue().getRight();

        int[] weightProperties = dimensions.relProperties().allPropertyKeyIds();
        double[] defaultWeights = dimensions.relProperties().allDefaultWeights();

        LongAdder relationshipCounter = new LongAdder();
        AdjacencyBuilder outBuilder = AdjacencyBuilder.compressing(
            outRelationshipsBuilder,
            numberOfPages,
            pageSize,
            tracker,
            relationshipCounter,
            weightProperties,
            defaultWeights);
        AdjacencyBuilder inBuilder = AdjacencyBuilder.compressing(
            inRelationshipsBuilder,
            numberOfPages,
            pageSize,
            tracker,
            relationshipCounter,
            weightProperties,
            defaultWeights);

        RelationshipImporter importer = new RelationshipImporter(
            setup.tracker,
            outBuilder,
            setup.loadAsUndirected ? outBuilder : inBuilder
        );

        return new SingleTypeRelationshipImporter.Builder(mapping, importer, relationshipCounter);
    }
}

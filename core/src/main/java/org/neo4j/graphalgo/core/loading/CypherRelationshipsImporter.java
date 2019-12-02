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

import com.carrotsearch.hppc.ObjectLongHashMap;
import com.carrotsearch.hppc.ObjectLongMap;
import org.apache.commons.lang3.tuple.Pair;
import org.neo4j.graphalgo.PropertyMapping;
import org.neo4j.graphalgo.RelationshipTypeMapping;
import org.neo4j.graphalgo.api.GraphSetup;
import org.neo4j.graphalgo.core.GraphDimensions;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toMap;
import static org.neo4j.graphalgo.core.utils.ParallelUtil.DEFAULT_BATCH_SIZE;

class CypherRelationshipsImporter extends CypherRecordLoader<ObjectLongMap<RelationshipTypeMapping>> {

    private final IdMap idMap;
    private final GraphDimensions dimensions;
    private final int relationshipPropertyCount;

    private final Context importerContext;
    private final Map<RelationshipTypeMapping, SingleTypeRelationshipImporter.Builder.WithImporter> relationshipImporterBuilders;
    private final Map<String, Integer> propertyKeyIdsByName;
    private final Map<String, Double> propertyDefaultValueByName;

    private final Map<RelationshipTypeMapping, LongAdder> allRelationshipCounters;

    CypherRelationshipsImporter(
        IdMap idMap,
        Map<RelationshipTypeMapping, Pair<RelationshipsBuilder, RelationshipsBuilder>> allBuilders,
        GraphDatabaseAPI api,
        GraphSetup setup,
        GraphDimensions dimensions
    ) {
        super(setup.relationshipType, idMap.nodeCount(), api, setup);
        this.idMap = idMap;

        this.dimensions = dimensions;

        ImportSizing importSizing = ImportSizing.of(setup.concurrency, idMap.nodeCount());
        int pageSize = importSizing.pageSize();
        int numberOfPages = importSizing.numberOfPages();

        boolean importWeights = dimensions.relProperties().atLeastOneExists();

        this.relationshipPropertyCount = dimensions.relProperties().numberOfMappings();
        this.propertyKeyIdsByName = dimensions
            .relProperties()
            .stream()
            .collect(toMap(PropertyMapping::neoPropertyKey, PropertyMapping::propertyKeyId));
        this.propertyDefaultValueByName = dimensions
            .relProperties()
            .stream()
            .collect(toMap(PropertyMapping::neoPropertyKey, PropertyMapping::defaultValue));

        Map<RelationshipTypeMapping, SingleTypeRelationshipImporter.Builder> importerBuilders = allBuilders
            .entrySet()
            .stream()
            .collect(toMap(
                Map.Entry::getKey,
                entry ->
                    createImporterBuilder(pageSize, numberOfPages, entry, setup.tracker)
            ));

        this.allRelationshipCounters = new HashMap<>();
        for (SingleTypeRelationshipImporter.Builder importerBuilder : importerBuilders.values()) {
            this.allRelationshipCounters.put(importerBuilder.mapping(), importerBuilder.relationshipCounter());
        }

        this.relationshipImporterBuilders = importerBuilders
            .entrySet()
            .stream()
            .collect(toMap(
                Map.Entry::getKey,
                entry -> entry.getValue()
                    .loadImporter(false, true, false, importWeights)
            ));

        this.importerContext = new Context();
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
                    RelationshipPropertiesBatchBuffer propertyReader = new RelationshipPropertiesBatchBuffer(
                        batchSize == CypherLoadingUtils.NO_BATCHING ? DEFAULT_BATCH_SIZE : batchSize,
                        relationshipPropertyCount
                    );
                    SingleTypeRelationshipImporter importer = builder.withBuffer(idMap, bufferSize, propertyReader);
                    return new MultiRelationshipRowVisitor.Context(relationshipTypeMapping, importer, propertyReader);
                }
            ).collect(Collectors.toList());

        MultiRelationshipRowVisitor visitor = new MultiRelationshipRowVisitor(
            idMap,
            importerContext,
            propertyKeyIdsByName,
            propertyDefaultValueByName,
            bufferSize
        );

        runLoadingQuery(offset, batchSize, visitor);
        visitor.flushAll();
        return new BatchLoadResult(offset, visitor.rows(), -1L, visitor.relationshipCount());
    }

    @Override
    void updateCounts(BatchLoadResult result) { }

    @Override
    ObjectLongMap<RelationshipTypeMapping> result() {
        List<Runnable> flushTasks = relationshipImporterBuilders
            .values()
            .stream()
            .flatMap(SingleTypeRelationshipImporter.Builder.WithImporter::flushTasks)
            .collect(Collectors.toList());

        ParallelUtil.run(flushTasks, setup.executor);

        ObjectLongMap<RelationshipTypeMapping> relationshipCounters = new ObjectLongHashMap<>(allRelationshipCounters.size());
        allRelationshipCounters.forEach((mapping, counter) -> relationshipCounters.put(mapping, counter.sum()));
        return relationshipCounters;
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

    static class Context {

        private final Map<String, SingleTypeRelationshipImporter.Builder> importerBuildersByType;

        public Context() {
            importerBuildersByType = new HashMap<>();
        }

        public SingleTypeRelationshipImporter.Builder getOrCreateImporterBuilder(String relationshipType) {
            SingleTypeRelationshipImporter.Builder importer;
            if (importerBuildersByType.containsKey(relationshipType)) {
                importer = importerBuildersByType.get(relationshipType);
            } else {
                importer = createImporter(relationshipType);
                importerBuildersByType.put(relationshipType, importer);
            }
            return importer;
        }

        private SingleTypeRelationshipImporter.Builder createImporter(String relationshipType) {
            return null;
        }
    }
}

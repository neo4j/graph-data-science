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
import org.neo4j.graphalgo.core.DeduplicationStrategy;
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

class CypherRelationshipsImporter extends CypherRecordLoader<ObjectLongMap<RelationshipTypeMapping>> {

    private final IdMap idMap;
    private final GraphDimensions dimensions;

    private final Context importerContext;
    private final Map<String, Integer> propertyKeyIdsByName;
    private final Map<String, Double> propertyDefaultValueByName;

    private final Map<RelationshipTypeMapping, LongAdder> allRelationshipCounters;

    CypherRelationshipsImporter(
        IdMap idMap,
        GraphDatabaseAPI api,
        GraphSetup setup,
        GraphDimensions dimensions
    ) {
        super(setup.relationshipType(), idMap.nodeCount(), api, setup);
        this.idMap = idMap;

        this.dimensions = dimensions;

        this.propertyKeyIdsByName = dimensions
            .relProperties()
            .stream()
            .collect(toMap(PropertyMapping::neoPropertyKey, PropertyMapping::propertyKeyId));
        this.propertyDefaultValueByName = dimensions
            .relProperties()
            .stream()
            .collect(toMap(PropertyMapping::neoPropertyKey, PropertyMapping::defaultValue));

        this.allRelationshipCounters = new HashMap<>();
        this.importerContext = new Context();
    }

    @Override
    BatchLoadResult loadOneBatch(long offset, int batchSize, int bufferSize) {
        MultiRelationshipRowVisitor visitor = new MultiRelationshipRowVisitor(
            idMap,
            importerContext,
            propertyKeyIdsByName,
            propertyDefaultValueByName,
            batchSize,
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
        List<Runnable> flushTasks = importerContext.importerBuildersByType
            .values()
            .stream()
            .flatMap(SingleTypeRelationshipImporter.Builder.WithImporter::flushTasks)
            .collect(Collectors.toList());

        ParallelUtil.run(flushTasks, setup.executor());

        ObjectLongMap<RelationshipTypeMapping> relationshipCounters = new ObjectLongHashMap<>(allRelationshipCounters.size());
        allRelationshipCounters.forEach((mapping, counter) -> relationshipCounters.put(mapping, counter.sum()));
        return relationshipCounters;
    }

    Map<RelationshipTypeMapping, Pair<RelationshipsBuilder, RelationshipsBuilder>> allBuilders() {
        return importerContext.allBuilders;
    }

    private SingleTypeRelationshipImporter.Builder createImporterBuilder(
        int pageSize,
        int numberOfPages,
        RelationshipTypeMapping mapping,
        RelationshipsBuilder outgoingRelationshipsBuilder,
        RelationshipsBuilder incomingRelationshipsBuilder,
        AllocationTracker tracker
    ) {

        int[] weightProperties = dimensions.relProperties().allPropertyKeyIds();
        double[] defaultWeights = dimensions.relProperties().allDefaultWeights();

        LongAdder relationshipCounter = new LongAdder();
        AdjacencyBuilder outBuilder = AdjacencyBuilder.compressing(
            outgoingRelationshipsBuilder,
            numberOfPages,
            pageSize,
            tracker,
            relationshipCounter,
            weightProperties,
            defaultWeights);
        AdjacencyBuilder inBuilder = AdjacencyBuilder.compressing(
            incomingRelationshipsBuilder,
            numberOfPages,
            pageSize,
            tracker,
            relationshipCounter,
            weightProperties,
            defaultWeights);

        RelationshipImporter importer = new RelationshipImporter(
            setup.tracker(),
            outBuilder,
            setup.loadAsUndirected() ? outBuilder : inBuilder
        );

        return new SingleTypeRelationshipImporter.Builder(mapping, importer, relationshipCounter);
    }

    class Context {

        private final Map<String, SingleTypeRelationshipImporter.Builder.WithImporter> importerBuildersByType;
        private final Map<RelationshipTypeMapping, Pair<RelationshipsBuilder, RelationshipsBuilder>> allBuilders;

        final int pageSize;
        final int numberOfPages;
        final boolean importWeights;

        Context() {
            this.importerBuildersByType = new HashMap<>();
            this.allBuilders = new HashMap<>();

            this.importWeights = dimensions.relProperties().atLeastOneExists();

            ImportSizing importSizing = ImportSizing.of(setup.concurrency(), idMap.nodeCount());
            this.pageSize = importSizing.pageSize();
            this.numberOfPages = importSizing.numberOfPages();
        }

        synchronized SingleTypeRelationshipImporter.Builder.WithImporter getOrCreateImporterBuilder(String relationshipType) {
            SingleTypeRelationshipImporter.Builder.WithImporter importerBuilder;
            if (importerBuildersByType.containsKey(relationshipType)) {
                importerBuilder = importerBuildersByType.get(relationshipType);
            } else {
                RelationshipTypeMapping typeMapping = RelationshipTypeMapping.of(relationshipType, -1);

                importerBuilder = createImporter(typeMapping);
                importerBuildersByType.put(relationshipType, importerBuilder);
            }
            return importerBuilder;
        }

        private SingleTypeRelationshipImporter.Builder.WithImporter createImporter(RelationshipTypeMapping typeMapping) {
            Pair<RelationshipsBuilder, RelationshipsBuilder> buildersForRelationshipType = createBuildersForRelationshipType(
                setup.tracker());

            allBuilders.put(typeMapping, buildersForRelationshipType);

            SingleTypeRelationshipImporter.Builder importerBuilder = createImporterBuilder(
                pageSize,
                numberOfPages,
                typeMapping,
                buildersForRelationshipType.getLeft(),
                buildersForRelationshipType.getRight(),
                setup.tracker()
            );
            allRelationshipCounters.put(typeMapping, importerBuilder.relationshipCounter());
            return importerBuilder.loadImporter(
                false,
                true,
                false,
                importWeights
            );
        }

        private Pair<RelationshipsBuilder, RelationshipsBuilder> createBuildersForRelationshipType(AllocationTracker tracker) {
            RelationshipsBuilder outgoingRelationshipsBuilder = null;
            RelationshipsBuilder incomingRelationshipsBuilder = null;

            DeduplicationStrategy[] deduplicationStrategies = dimensions
                .relProperties()
                .stream()
                .map(property -> property.deduplicationStrategy() == DeduplicationStrategy.DEFAULT
                    ? DeduplicationStrategy.NONE
                    : property.deduplicationStrategy()
                )
                .toArray(DeduplicationStrategy[]::new);
            // TODO: backwards compat code
            if (deduplicationStrategies.length == 0) {
                DeduplicationStrategy deduplicationStrategy =
                    setup.deduplicationStrategy() == DeduplicationStrategy.DEFAULT
                        ? DeduplicationStrategy.NONE
                        : setup.deduplicationStrategy();
                deduplicationStrategies = new DeduplicationStrategy[]{deduplicationStrategy};
            }

            if (setup.loadAsUndirected()) {
                outgoingRelationshipsBuilder = new RelationshipsBuilder(
                    deduplicationStrategies,
                    tracker,
                    setup.relationshipPropertyMappings().numberOfMappings());
            } else {
                if (setup.loadOutgoing()) {
                    outgoingRelationshipsBuilder = new RelationshipsBuilder(
                        deduplicationStrategies,
                        tracker,
                        setup.relationshipPropertyMappings().numberOfMappings());
                }
                if (setup.loadIncoming()) {
                    incomingRelationshipsBuilder = new RelationshipsBuilder(
                        deduplicationStrategies,
                        tracker,
                        setup.relationshipPropertyMappings().numberOfMappings());
                }
            }

            return Pair.of(outgoingRelationshipsBuilder, incomingRelationshipsBuilder);
        }
    }
}

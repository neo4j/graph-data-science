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
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.commons.lang3.tuple.Pair;
import org.neo4j.graphalgo.PropertyMapping;
import org.neo4j.graphalgo.RelationshipTypeMapping;
import org.neo4j.graphalgo.api.GraphSetup;
import org.neo4j.graphalgo.core.DeduplicationStrategy;
import org.neo4j.graphalgo.core.GraphDimensions;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphdb.Result;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toMap;

class CypherRelationshipLoader extends CypherRecordLoader<ObjectLongMap<RelationshipTypeMapping>, RelationshipRowVisitor> {

    private final IdMap idMap;
    private final GraphDimensions dimensions;

    private final Context loaderContext;

    // Property mappings are either defined upfront in
    // the procedure configuration or during load time
    // by looking at the columns returned by the query.
    private Map<String, Integer> propertyKeyIdsByName;
    private Map<String, Double> propertyDefaultValueByName;
    private int[] propertyKeyIds;
    private double[] propertyDefaultValues;

    private final Map<RelationshipTypeMapping, LongAdder> allRelationshipCounters;
    private final DeduplicationStrategy[] deduplicationStrategies;

    CypherRelationshipLoader(
        IdMap idMap,
        GraphDatabaseAPI api,
        GraphSetup setup,
        GraphDimensions dimensions
    ) {
        super(setup.relationshipType(), idMap.nodeCount(), api, setup);
        this.idMap = idMap;

        this.dimensions = dimensions;

        MutableInt propertyKeyId = new MutableInt(0);

        this.propertyKeyIdsByName = dimensions
            .relProperties()
            .stream()
            .collect(toMap(PropertyMapping::propertyKey, unused -> propertyKeyId.getAndIncrement()));
        this.propertyDefaultValueByName = dimensions
            .relProperties()
            .stream()
            .collect(toMap(PropertyMapping::propertyKey, PropertyMapping::defaultValue));

        this.propertyKeyIds = dimensions.relProperties().allPropertyKeyIds();
        this.propertyDefaultValues = dimensions.relProperties().allDefaultWeights();

        this.allRelationshipCounters = new HashMap<>();
        this.loaderContext = new Context();

        this.deduplicationStrategies = getDeduplicationStrategies();
    }

    @Override
    void runLoadingQuery(long offset, int batchSize, RelationshipRowVisitor visitor) {
        Map<String, Object> parameters =
            batchSize == CypherLoadingUtils.NO_BATCHING
                ? setup.params()
                : CypherLoadingUtils.params(setup.params(), offset, batchSize);
        Result result = api.execute(loadQuery, parameters);

        List<String> allColumns = result.columns();

        boolean isAnyRelTypeQuery = !allColumns.contains(RelationshipRowVisitor.TYPE_COLUMN);
        visitor.isAnyTypeResult(isAnyRelTypeQuery);

        if (isAnyRelTypeQuery) {
            loaderContext.getOrCreateImporterBuilder(RelationshipTypeMapping.all());
        }

        // If the user specifies property mappings,
        // we use those. Otherwise, we create new
        // property mappings from the result columns.
        if (!dimensions.relProperties().hasMappings()) {
            Predicate<String> contains = RelationshipRowVisitor.RESERVED_COLUMNS::contains;
            MutableInt propertyKeyId = new MutableInt(0);
            List<String> propertyColumns = allColumns.stream().filter(contains.negate()).collect(Collectors.toList());
            double defaultValue = 0D;

            propertyKeyIdsByName = propertyColumns
                .stream()
                .collect(Collectors.toMap(
                    propertyColumn -> propertyColumn,
                    propertyColumn -> propertyKeyId.getAndIncrement()
                ));

            propertyDefaultValueByName = propertyColumns
                .stream()
                .collect(Collectors.toMap(
                    propertyColumn -> propertyColumn,
                    propertyColumn -> defaultValue
                ));

            propertyKeyIds = IntStream.range(0, propertyColumns.size()).toArray();
            propertyDefaultValues = IntStream.range(0, propertyColumns.size()).mapToDouble(i -> defaultValue).toArray();

            // TODO: we need to set the duplication strategies to NONE
        }

        result.accept(visitor);
    }

    @Override
    BatchLoadResult loadOneBatch(long offset, int batchSize, int bufferSize) {
        RelationshipRowVisitor visitor = new RelationshipRowVisitor(
            idMap,
            loaderContext,
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
        List<Runnable> flushTasks = loaderContext.importerBuildersByType
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
        return loaderContext.allBuilders;
    }

    private DeduplicationStrategy[] getDeduplicationStrategies() {
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
        return deduplicationStrategies;
    }

    class Context {

        private final Map<RelationshipTypeMapping, SingleTypeRelationshipImporter.Builder.WithImporter> importerBuildersByType;
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

        synchronized SingleTypeRelationshipImporter.Builder.WithImporter getOrCreateImporterBuilder(RelationshipTypeMapping relationshipTypeMapping) {
            SingleTypeRelationshipImporter.Builder.WithImporter importerBuilder;
            if (importerBuildersByType.containsKey(relationshipTypeMapping)) {
                importerBuilder = importerBuildersByType.get(relationshipTypeMapping);
            } else {
                importerBuilder = createImporter(relationshipTypeMapping);
                importerBuildersByType.put(relationshipTypeMapping, importerBuilder);
            }
            return importerBuilder;
        }

        private SingleTypeRelationshipImporter.Builder.WithImporter createImporter(RelationshipTypeMapping typeMapping) {
            Pair<RelationshipsBuilder, RelationshipsBuilder> builders = createBuildersForRelationshipType(setup.tracker());

            allBuilders.put(typeMapping, builders);

            SingleTypeRelationshipImporter.Builder importerBuilder = createImporterBuilder(
                pageSize,
                numberOfPages,
                typeMapping,
                builders.getLeft(),
                builders.getRight(),
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

            if (setup.loadAsUndirected()) {
                outgoingRelationshipsBuilder = new RelationshipsBuilder(
                    deduplicationStrategies,
                    tracker,
                    setup.relationshipPropertyMappings().numberOfMappings()
                );
            } else {
                if (setup.loadOutgoing()) {
                    outgoingRelationshipsBuilder = new RelationshipsBuilder(
                        deduplicationStrategies,
                        tracker,
                        setup.relationshipPropertyMappings().numberOfMappings()
                    );
                }
                if (setup.loadIncoming()) {
                    incomingRelationshipsBuilder = new RelationshipsBuilder(
                        deduplicationStrategies,
                        tracker,
                        setup.relationshipPropertyMappings().numberOfMappings()
                    );
                }
            }

            return Pair.of(outgoingRelationshipsBuilder, incomingRelationshipsBuilder);
        }

        private SingleTypeRelationshipImporter.Builder createImporterBuilder(
            int pageSize,
            int numberOfPages,
            RelationshipTypeMapping mapping,
            RelationshipsBuilder outgoingRelationshipsBuilder,
            RelationshipsBuilder incomingRelationshipsBuilder,
            AllocationTracker tracker
        ) {
            LongAdder relationshipCounter = new LongAdder();
            AdjacencyBuilder outBuilder = AdjacencyBuilder.compressing(
                outgoingRelationshipsBuilder,
                numberOfPages,
                pageSize,
                tracker,
                relationshipCounter,
                propertyKeyIds,
                propertyDefaultValues
            );
            AdjacencyBuilder inBuilder = AdjacencyBuilder.compressing(
                incomingRelationshipsBuilder,
                numberOfPages,
                pageSize,
                tracker,
                relationshipCounter,
                propertyKeyIds,
                propertyDefaultValues
            );

            RelationshipImporter importer = new RelationshipImporter(
                setup.tracker(),
                outBuilder,
                setup.loadAsUndirected() ? outBuilder : inBuilder
            );

            return new SingleTypeRelationshipImporter.Builder(mapping, importer, relationshipCounter);
        }
    }
}

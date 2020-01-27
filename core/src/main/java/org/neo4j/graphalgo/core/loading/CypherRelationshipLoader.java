/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
import org.eclipse.collections.impl.map.mutable.primitive.ObjectDoubleHashMap;
import org.eclipse.collections.impl.map.mutable.primitive.ObjectIntHashMap;
import org.immutables.value.Value;
import org.neo4j.graphalgo.PropertyMapping;
import org.neo4j.graphalgo.RelationshipProjectionMapping;
import org.neo4j.graphalgo.RelationshipProjectionMappings;
import org.neo4j.graphalgo.ResolvedPropertyMapping;
import org.neo4j.graphalgo.ResolvedPropertyMappings;
import org.neo4j.graphalgo.annotation.ValueClass;
import org.neo4j.graphalgo.api.GraphSetup;
import org.neo4j.graphalgo.core.Aggregation;
import org.neo4j.graphalgo.core.GraphDimensions;
import org.neo4j.graphalgo.core.ImmutableGraphDimensions;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphdb.Result;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;

import static org.neo4j.graphalgo.PropertyMapping.DEFAULT_FALLBACK_VALUE;
import static org.neo4j.graphalgo.core.Aggregation.NONE;
import static org.neo4j.kernel.api.StatementConstants.NO_SUCH_PROPERTY_KEY;

@Value.Enclosing
class CypherRelationshipLoader extends CypherRecordLoader<CypherRelationshipLoader.LoadResult> {

    private final IdMap idMap;
    private final Context loaderContext;
    private final GraphDimensions outerDimensions;
    private final boolean hasExplicitPropertyMappings;
    private final Aggregation globalAggregation;
    private final double globalDefaultPropertyValue;
    private final Map<RelationshipProjectionMapping, LongAdder> relationshipCounters;

    // Property mappings are either defined upfront in
    // the procedure configuration or during load time
    // by looking at the columns returned by the query.
    private ObjectIntHashMap<String> propertyKeyIdsByName;
    private ObjectDoubleHashMap<String> propertyDefaultValueByName;
    private boolean importWeights;
    private int[] propertyKeyIds;
    private double[] propertyDefaultValues;
    private Aggregation[] aggregations;
    private boolean initializedFromResult;

    private GraphDimensions resultDimensions;

    CypherRelationshipLoader(
        String relationshipQuery,
        IdMap idMap,
        GraphDatabaseAPI api,
        GraphSetup setup,
        GraphDimensions dimensions
    ) {
        super(relationshipQuery, idMap.nodeCount(), api, setup);
        this.idMap = idMap;
        this.outerDimensions = dimensions;
        this.loaderContext = new Context();
        this.relationshipCounters = new HashMap<>();

        this.hasExplicitPropertyMappings = dimensions.relationshipProperties().hasMappings();

        this.globalAggregation = setup.aggregation() == Aggregation.DEFAULT
            ? NONE
            : setup.aggregation();
        this.globalDefaultPropertyValue = setup.relationshipDefaultPropertyValue().orElse(DEFAULT_FALLBACK_VALUE);

        this.resultDimensions = initFromDimension(dimensions);
    }

    private GraphDimensions initFromDimension(GraphDimensions dimensions) {
        MutableInt propertyKeyId = new MutableInt(0);

        int numberOfMappings = dimensions.relationshipProperties().numberOfMappings();
        propertyKeyIdsByName = new ObjectIntHashMap<>(numberOfMappings);
        dimensions
            .relationshipProperties()
            .stream()
            .forEach(mapping -> propertyKeyIdsByName.put(mapping.neoPropertyKey(), propertyKeyId.getAndIncrement()));
        propertyDefaultValueByName = new ObjectDoubleHashMap<>(numberOfMappings);
        dimensions
            .relationshipProperties()
            .stream()
            .forEach(mapping -> propertyDefaultValueByName.put(mapping.neoPropertyKey(), mapping.defaultValue()));

        // We can not rely on what the token store gives us.
        // We need to resolve the given property mappings
        // using our newly created property key identifiers.
        GraphDimensions newDimensions = ImmutableGraphDimensions.builder()
            .from(dimensions)
            .relationshipProperties(ResolvedPropertyMappings.of(dimensions.relationshipProperties().stream()
                .map(mapping -> PropertyMapping.of(
                    mapping.propertyKey(),
                    mapping.neoPropertyKey(),
                    mapping.defaultValue(),
                    mapping.aggregation()
                ))
                .map(mapping -> mapping.resolveWith(propertyKeyIdsByName.get(mapping.neoPropertyKey())))
                .collect(Collectors.toList())))
            .build();

        importWeights = newDimensions.relationshipProperties().atLeastOneExists();
        propertyKeyIds = newDimensions.relationshipProperties().allPropertyKeyIds();
        propertyDefaultValues = newDimensions.relationshipProperties().allDefaultWeights();
        aggregations = getAggregations(newDimensions);

        return newDimensions;
    }

    @Override
    BatchLoadResult loadOneBatch(long offset, int batchSize, int bufferSize) {
        Result queryResult = runLoadingQuery(offset, batchSize);

        List<String> allColumns = queryResult.columns();

        // If the user specifies property mappings, we use those.
        // Otherwise, we create new property mappings from the result columns.
        // We do that only once, as each batch has the same columns.
        if (!hasExplicitPropertyMappings && !initializedFromResult) {

            Collection<String> propertyColumns = getPropertyColumns(queryResult);

            List<ResolvedPropertyMapping> propertyMappings = propertyColumns
                .stream()
                .map(propertyColumn -> PropertyMapping.of(
                    propertyColumn,
                    propertyColumn,
                    globalDefaultPropertyValue,
                    globalAggregation
                ))
                .map(mapping -> mapping.resolveWith(NO_SUCH_PROPERTY_KEY))
                .collect(Collectors.toList());

            GraphDimensions innerDimensions = ImmutableGraphDimensions.builder()
                .from(outerDimensions)
                .relationshipProperties(ResolvedPropertyMappings.of(propertyMappings))
                .build();

            resultDimensions = initFromDimension(innerDimensions);

            initializedFromResult = true;
        }

        boolean isAnyRelTypeQuery = !allColumns.contains(RelationshipRowVisitor.TYPE_COLUMN);

        if (isAnyRelTypeQuery) {
            loaderContext.getOrCreateImporterBuilder(RelationshipProjectionMapping.all());
        }

        RelationshipRowVisitor visitor = new RelationshipRowVisitor(
            idMap,
            loaderContext,
            propertyKeyIdsByName,
            propertyDefaultValueByName,
            bufferSize,
            isAnyRelTypeQuery
        );

        queryResult.accept(visitor);
        visitor.flushAll();
        return new BatchLoadResult(offset, visitor.rows(), -1L, visitor.relationshipCount());
    }

    @Override
    void updateCounts(BatchLoadResult result) { }

    @Override
    LoadResult result() {
        List<Runnable> flushTasks = loaderContext.importerBuildersByType
            .values()
            .stream()
            .flatMap(SingleTypeRelationshipImporter.Builder.WithImporter::flushTasks)
            .collect(Collectors.toList());

        ParallelUtil.run(flushTasks, setup.executor());

        ObjectLongMap<RelationshipProjectionMapping> relationshipCounters = new ObjectLongHashMap<>(this.relationshipCounters.size());
        this.relationshipCounters.forEach((mapping, counter) -> relationshipCounters.put(mapping, counter.sum()));

        resultDimensions = ImmutableGraphDimensions.builder().from(resultDimensions)
            .relationshipProjectionMappings(RelationshipProjectionMappings.of(relationshipCounters.keys().toArray(
                RelationshipProjectionMapping.class)))
            .build();

        return ImmutableCypherRelationshipLoader.LoadResult.builder()
            .dimensions(resultDimensions)
            .relationshipCounts(relationshipCounters)
            .build();
    }

    @Override
    Set<String> getReservedColumns() {
        return RelationshipRowVisitor.RESERVED_COLUMNS;
    }

    Map<RelationshipProjectionMapping, RelationshipsBuilder> allBuilders() {
        return loaderContext.allBuilders;
    }

    private Aggregation[] getAggregations(GraphDimensions dimensions) {
        Aggregation[] aggregations = dimensions
            .relationshipProperties()
            .stream()
            .map(property -> property.aggregation() == Aggregation.DEFAULT
                ? NONE
                : property.aggregation()
            )
            .toArray(Aggregation[]::new);
        // TODO: backwards compat code
        if (aggregations.length == 0) {
            aggregations = new Aggregation[]{globalAggregation};
        }
        return aggregations;
    }

    class Context {

        private final Map<RelationshipProjectionMapping, SingleTypeRelationshipImporter.Builder.WithImporter> importerBuildersByType;
        private final Map<RelationshipProjectionMapping, RelationshipsBuilder> allBuilders;

        private final int pageSize;
        private final int numberOfPages;

        Context() {
            this.importerBuildersByType = new HashMap<>();
            this.allBuilders = new HashMap<>();

            ImportSizing importSizing = ImportSizing.of(setup.concurrency(), idMap.nodeCount());
            this.pageSize = importSizing.pageSize();
            this.numberOfPages = importSizing.numberOfPages();
        }

        synchronized SingleTypeRelationshipImporter.Builder.WithImporter getOrCreateImporterBuilder(
            RelationshipProjectionMapping relationshipProjectionMapping
        ) {
            return importerBuildersByType.computeIfAbsent(relationshipProjectionMapping, this::createImporter);
        }

        private SingleTypeRelationshipImporter.Builder.WithImporter createImporter(RelationshipProjectionMapping typeMapping) {
            RelationshipsBuilder builder = new RelationshipsBuilder(
                aggregations,
                setup.tracker(),
                propertyKeyIds.length
            );

            allBuilders.put(typeMapping, builder);

            SingleTypeRelationshipImporter.Builder importerBuilder = createImporterBuilder(
                pageSize,
                numberOfPages,
                typeMapping,
                builder,
                setup.tracker()
            );

            relationshipCounters.put(typeMapping, importerBuilder.relationshipCounter());

            return importerBuilder.loadImporter(
                false,
                true,
                false,
                importWeights
            );
        }

        private SingleTypeRelationshipImporter.Builder createImporterBuilder(
            int pageSize,
            int numberOfPages,
            RelationshipProjectionMapping mapping,
            RelationshipsBuilder relationshipsBuilder,
            AllocationTracker tracker
        ) {
            LongAdder relationshipCounter = new LongAdder();
            AdjacencyBuilder adjacencyBuilder = AdjacencyBuilder.compressing(
                relationshipsBuilder,
                numberOfPages,
                pageSize,
                tracker,
                relationshipCounter,
                propertyKeyIds,
                propertyDefaultValues
            );

            RelationshipImporter relationshipImporter = new RelationshipImporter(
                setup.tracker(),
                adjacencyBuilder,
                null
            );

            return new SingleTypeRelationshipImporter.Builder(mapping, relationshipImporter, relationshipCounter);
        }
    }

    @ValueClass
    interface LoadResult {
        GraphDimensions dimensions();

        ObjectLongMap<RelationshipProjectionMapping> relationshipCounts();
    }
}

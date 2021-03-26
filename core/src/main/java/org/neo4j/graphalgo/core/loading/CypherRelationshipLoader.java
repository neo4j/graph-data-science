/*
 * Copyright (c) "Neo4j"
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
import org.neo4j.graphalgo.Orientation;
import org.neo4j.graphalgo.PropertyMapping;
import org.neo4j.graphalgo.PropertyMappings;
import org.neo4j.graphalgo.RelationshipProjection;
import org.neo4j.graphalgo.RelationshipType;
import org.neo4j.graphalgo.annotation.ValueClass;
import org.neo4j.graphalgo.api.GraphLoaderContext;
import org.neo4j.graphalgo.api.NodeMapping;
import org.neo4j.graphalgo.config.GraphCreateFromCypherConfig;
import org.neo4j.graphalgo.core.Aggregation;
import org.neo4j.graphalgo.core.GraphDimensions;
import org.neo4j.graphalgo.core.ImmutableGraphDimensions;
import org.neo4j.graphalgo.core.concurrency.ParallelUtil;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;

import static org.neo4j.graphalgo.RelationshipType.ALL_RELATIONSHIPS;
import static org.neo4j.graphalgo.core.loading.construction.NodesBuilder.NO_PROPERTY_VALUE;
import static org.neo4j.graphalgo.utils.GdsFeatureToggles.USE_PRE_AGGREGATION;
import static org.neo4j.kernel.api.StatementConstants.NO_SUCH_RELATIONSHIP_TYPE;

@Value.Enclosing
class CypherRelationshipLoader extends CypherRecordLoader<CypherRelationshipLoader.LoadResult> {

    private final NodeMapping nodeMapping;
    private final Context loaderContext;
    private final GraphDimensions dimensionsAfterNodeLoading;
    private final Map<RelationshipProjection, LongAdder> relationshipCounters;

    // Property mappings are either defined upfront in
    // the procedure configuration or during load time
    // by looking at the columns returned by the query.
    private ObjectIntHashMap<String> propertyKeyIdsByName;
    private ObjectDoubleHashMap<String> propertyDefaultValueByName;
    private boolean importProperties;
    private PropertyMappings propertyMappings;
    private int[] propertyKeyIds;
    private double[] propertyDefaultValues;
    private Aggregation[] aggregations;
    private boolean initializedFromResult;

    private GraphDimensions resultDimensions;

    CypherRelationshipLoader(
        String relationshipQuery,
        NodeMapping nodeMapping,
        GraphCreateFromCypherConfig config,
        GraphLoaderContext loadingContext,
        GraphDimensions dimensions
    ) {
        super(relationshipQuery, nodeMapping.nodeCount(), config, loadingContext);
        this.nodeMapping = nodeMapping;
        this.dimensionsAfterNodeLoading = dimensions;
        this.loaderContext = new Context();
        this.relationshipCounters = new HashMap<>();
    }

    private void initFromPropertyMappings(PropertyMappings propertyMappings) {
        this.propertyMappings = propertyMappings;

        MutableInt propertyKeyId = new MutableInt(0);

        int numberOfMappings = propertyMappings.numberOfMappings();
        propertyKeyIdsByName = new ObjectIntHashMap<>(numberOfMappings);
        propertyMappings
            .stream()
            .forEach(mapping -> propertyKeyIdsByName.put(mapping.neoPropertyKey(), propertyKeyId.getAndIncrement()));

        propertyDefaultValueByName = new ObjectDoubleHashMap<>(numberOfMappings);
        propertyMappings
            .stream()
            .forEach(mapping -> propertyDefaultValueByName.put(
                mapping.neoPropertyKey(),
                mapping.defaultValue().doubleValue()
            ));

        // We can not rely on what the token store gives us.
        // We need to resolve the given property mappings
        // using our newly created property key identifiers.
        ImmutableGraphDimensions.Builder dimensionsBuilder = ImmutableGraphDimensions.builder().from(
            dimensionsAfterNodeLoading);
        propertyMappings
            .forEach(propertyMapping -> dimensionsBuilder.putRelationshipPropertyToken(
                propertyMapping.neoPropertyKey(),
                propertyKeyIdsByName.get(propertyMapping.neoPropertyKey())
            ));
        GraphDimensions newDimensions = dimensionsBuilder.build();

        importProperties = !propertyMappings.isEmpty();
        propertyKeyIds = newDimensions.relationshipPropertyTokens().values().stream().mapToInt(i -> i).toArray();
        propertyDefaultValues = propertyMappings.mappings().stream().mapToDouble(propertyMapping -> propertyMapping.defaultValue().doubleValue()).toArray();
        aggregations = propertyMappings.mappings().stream().map(PropertyMapping::aggregation).toArray(Aggregation[]::new);
        if (propertyMappings.isEmpty()) {
            aggregations = new Aggregation[]{ Aggregation.NONE };
        }
        resultDimensions = newDimensions;
    }

    @Override
    BatchLoadResult loadSingleBatch(Transaction tx, int bufferSize) {
        Result queryResult = runLoadingQuery(tx);

        List<String> allColumns = queryResult.columns();

        // If the user specifies property mappings, we use those.
        // Otherwise, we create new property mappings from the result columns.
        // We do that only once, as each batch has the same columns.
        Collection<String> propertyColumns = getPropertyColumns(queryResult);
        if (!initializedFromResult) {

            List<PropertyMapping> propertyMappings = propertyColumns
                .stream()
                .map(propertyColumn -> PropertyMapping.of(
                    propertyColumn,
                    propertyColumn,
                    NO_PROPERTY_VALUE,
                    Aggregation.NONE
                ))
                .collect(Collectors.toList());

            initFromPropertyMappings(PropertyMappings.of(propertyMappings));

            initializedFromResult = true;
        }

        boolean isAnyRelTypeQuery = !allColumns.contains(RelationshipRowVisitor.TYPE_COLUMN);

        if (isAnyRelTypeQuery) {
            loaderContext.getOrCreateImporterBuilder(ALL_RELATIONSHIPS);
        }

        RelationshipRowVisitor visitor = new RelationshipRowVisitor(
            nodeMapping,
            loaderContext,
            propertyKeyIdsByName,
            propertyDefaultValueByName,
            bufferSize,
            isAnyRelTypeQuery,
            cypherConfig.validateRelationships()
        );

        queryResult.accept(visitor);
        visitor.flushAll();
        return new BatchLoadResult(visitor.rows(), -1L);
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

        ParallelUtil.run(flushTasks, loadingContext.executor());

        ObjectLongMap<RelationshipType> relationshipCounters = new ObjectLongHashMap<>(this.relationshipCounters.size());
        this.relationshipCounters.forEach((mapping, counter) -> relationshipCounters.put(
            RelationshipType.of(mapping.type()),
            counter.sum()
        ));

        return ImmutableCypherRelationshipLoader.LoadResult.builder()
            .dimensions(resultDimensions)
            .relationshipCounts(relationshipCounters)
            .build();
    }

    @Override
    Set<String> getMandatoryColumns() {
        return RelationshipRowVisitor.REQUIRED_COLUMNS;
    }

    @Override
    Set<String> getReservedColumns() {
        return RelationshipRowVisitor.RESERVED_COLUMNS;
    }

    @Override
    QueryType queryType() {
        return QueryType.RELATIONSHIP;
    }

    Map<RelationshipType, AdjacencyListWithPropertiesBuilder> allBuilders() {
        return loaderContext.allBuilders;
    }

    class Context {

        private final Map<RelationshipType, SingleTypeRelationshipImporter.Builder.WithImporter> importerBuildersByType;
        private final Map<RelationshipType, AdjacencyListWithPropertiesBuilder> allBuilders;

        private final int pageSize;
        private final int numberOfPages;

        Context() {
            this.importerBuildersByType = new HashMap<>();
            this.allBuilders = new HashMap<>();

            ImportSizing importSizing = ImportSizing.of(cypherConfig.readConcurrency(), nodeMapping.nodeCount());
            this.pageSize = importSizing.pageSize();
            this.numberOfPages = importSizing.numberOfPages();
        }

        synchronized SingleTypeRelationshipImporter.Builder.WithImporter getOrCreateImporterBuilder(
            RelationshipType relationshipType
        ) {
            return importerBuildersByType.computeIfAbsent(relationshipType, this::createImporter);
        }

        private SingleTypeRelationshipImporter.Builder.WithImporter createImporter(RelationshipType relationshipType) {
            RelationshipProjection projection = RelationshipProjection
                .builder()
                .type(relationshipType.name)
                .orientation(Orientation.NATURAL)
                .properties(propertyMappings)
                .build();

            Aggregation[] aggregationsWithDefault = Arrays.stream(aggregations)
                .map(Aggregation::resolve)
                .toArray(Aggregation[]::new);

            AdjacencyListWithPropertiesBuilder builder = AdjacencyListWithPropertiesBuilder.create(
                nodeMapping.nodeCount(),
                projection,
                TransientAdjacencyListBuilder.builderFactory(loadingContext.tracker()),
                aggregationsWithDefault,
                propertyKeyIds,
                propertyDefaultValues,
                loadingContext.tracker()
            );

            allBuilders.put(relationshipType, builder);

            SingleTypeRelationshipImporter.Builder importerBuilder = createImporterBuilder(
                pageSize,
                numberOfPages,
                relationshipType,
                projection,
                builder,
                loadingContext.tracker()
            );

            relationshipCounters.put(projection, importerBuilder.relationshipCounter());

            return importerBuilder.loadImporter(importProperties);
        }

        private SingleTypeRelationshipImporter.Builder createImporterBuilder(
            int pageSize,
            int numberOfPages,
            RelationshipType relationshipType,
            RelationshipProjection relationshipProjection,
            AdjacencyListWithPropertiesBuilder adjacencyListWithPropertiesBuilder,
            AllocationTracker tracker
        ) {
            LongAdder relationshipCounter = new LongAdder();
            AdjacencyBuilder adjacencyBuilder = AdjacencyBuilder.compressing(
                adjacencyListWithPropertiesBuilder,
                numberOfPages,
                pageSize,
                tracker,
                relationshipCounter,
                USE_PRE_AGGREGATION.isEnabled()
            );

            RelationshipImporter relationshipImporter = new RelationshipImporter(loadingContext.tracker(), adjacencyBuilder);
            return new SingleTypeRelationshipImporter.Builder(
                relationshipType,
                relationshipProjection,
                adjacencyBuilder.supportsProperties(),
                NO_SUCH_RELATIONSHIP_TYPE,
                relationshipImporter,
                relationshipCounter,
                cypherConfig.validateRelationships()
            );
        }
    }

    @ValueClass
    interface LoadResult {
        GraphDimensions dimensions();

        ObjectLongMap<RelationshipType> relationshipCounts();
    }
}

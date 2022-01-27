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
package org.neo4j.gds.core.loading;

import org.apache.commons.lang3.mutable.MutableInt;
import org.eclipse.collections.impl.map.mutable.primitive.ObjectDoubleHashMap;
import org.eclipse.collections.impl.map.mutable.primitive.ObjectIntHashMap;
import org.immutables.value.Value;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.PropertyMapping;
import org.neo4j.gds.PropertyMappings;
import org.neo4j.gds.RelationshipProjection;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.api.GraphLoaderContext;
import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.config.GraphProjectFromCypherConfig;
import org.neo4j.gds.core.Aggregation;
import org.neo4j.gds.core.GraphDimensions;
import org.neo4j.gds.core.ImmutableGraphDimensions;
import org.neo4j.gds.core.concurrency.ParallelUtil;
import org.neo4j.gds.core.loading.ThreadLocalSingleTypeRelationshipImporter.RelationshipTypeImportContext;
import org.neo4j.gds.core.loading.construction.NodesBuilder;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.utils.GdsFeatureToggles;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.neo4j.kernel.api.StatementConstants.NO_SUCH_RELATIONSHIP_TYPE;

@Value.Enclosing
class CypherRelationshipLoader extends CypherRecordLoader<CypherRelationshipLoader.LoadResult> {

    private final IdMap idMap;
    private final Context loaderContext;
    private final GraphDimensions dimensionsAfterNodeLoading;
    private final ProgressTracker progressTracker;

    // Property mappings are either defined upfront in
    // the procedure configuration or during load time
    // by looking at the columns returned by the query.
    private ObjectIntHashMap<String> propertyKeyIdsByName;
    private ObjectDoubleHashMap<String> propertyDefaultValueByName;
    private PropertyMappings propertyMappings;
    private int[] propertyKeyIds;
    private double[] propertyDefaultValues;
    private Aggregation[] aggregations;
    private boolean initializedFromResult;

    private GraphDimensions resultDimensions;

    CypherRelationshipLoader(
        String relationshipQuery,
        IdMap idMap,
        GraphProjectFromCypherConfig config,
        GraphLoaderContext loadingContext,
        GraphDimensions dimensions,
        ProgressTracker progressTracker
    ) {
        super(relationshipQuery, idMap.nodeCount(), config, loadingContext);
        this.idMap = idMap;
        this.dimensionsAfterNodeLoading = dimensions;
        this.progressTracker = progressTracker;
        this.loaderContext = new Context();
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

        propertyKeyIds = newDimensions.relationshipPropertyTokens().values().stream().mapToInt(i -> i).toArray();
        propertyDefaultValues = propertyMappings
            .mappings()
            .stream()
            .mapToDouble(propertyMapping -> propertyMapping.defaultValue().doubleValue())
            .toArray();
        aggregations = propertyMappings
            .mappings()
            .stream()
            .map(PropertyMapping::aggregation)
            .toArray(Aggregation[]::new);
        if (propertyMappings.isEmpty()) {
            aggregations = new Aggregation[]{Aggregation.NONE};
        }
        resultDimensions = newDimensions;
    }

    @Override
    BatchLoadResult loadSingleBatch(Transaction tx, int bufferSize) {
        progressTracker.beginSubTask("Relationships");
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
                    NodesBuilder.NO_PROPERTY_VALUE,
                    Aggregation.NONE
                ))
                .collect(Collectors.toList());

            initFromPropertyMappings(PropertyMappings.of(propertyMappings));

            initializedFromResult = true;
        }

        boolean isAnyRelTypeQuery = !allColumns.contains(RelationshipRowVisitor.TYPE_COLUMN);

        if (isAnyRelTypeQuery) {
            loaderContext.getOrCreateImporterFactory(RelationshipType.ALL_RELATIONSHIPS);
        }

        RelationshipRowVisitor visitor = new RelationshipRowVisitor(
            idMap,
            loaderContext,
            propertyKeyIdsByName,
            propertyDefaultValueByName,
            bufferSize,
            isAnyRelTypeQuery,
            cypherConfig.validateRelationships(),
            progressTracker
        );

        queryResult.accept(visitor);
        visitor.flushAll();
        progressTracker.endSubTask("Relationships");
        return new BatchLoadResult(visitor.rows(), -1L);
    }

    @Override
    void updateCounts(BatchLoadResult result) {}

    @Override
    LoadResult result() {
        var flushTasks = loaderContext.importerFactoriesByType
            .values()
            .stream()
            .map(RelationshipTypeImportContext::singleTypeRelationshipImporterFactory)
            .flatMap(factory -> factory.adjacencyListBuilderTasks().stream())
            .collect(Collectors.toList());

        ParallelUtil.run(flushTasks, loadingContext.executor());

        var relationshipsAndProperties = RelationshipsAndProperties.of(loaderContext.importerFactoriesByType.values());

        return ImmutableCypherRelationshipLoader.LoadResult.builder()
            .dimensions(resultDimensions)
            .relationshipsAndProperties(relationshipsAndProperties)
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

    class Context {

        private final Map<RelationshipType, RelationshipTypeImportContext> importerFactoriesByType;

        private final ImportSizing importSizing;

        Context() {
            this.importerFactoriesByType = new HashMap<>();
            this.importSizing = ImportSizing.of(cypherConfig.readConcurrency(), idMap.nodeCount());
        }

        synchronized ThreadLocalSingleTypeRelationshipImporter.Factory getOrCreateImporterFactory(RelationshipType relationshipType) {
            return importerFactoriesByType
                .computeIfAbsent(relationshipType, this::createImporterFactory)
                .singleTypeRelationshipImporterFactory();
        }

        private RelationshipTypeImportContext createImporterFactory(RelationshipType relationshipType) {
            RelationshipProjection projection = RelationshipProjection
                .builder()
                .type(relationshipType.name)
                .orientation(Orientation.NATURAL)
                .properties(propertyMappings)
                .build();

            Aggregation[] aggregationsWithDefault = Arrays.stream(aggregations)
                .map(Aggregation::resolve)
                .toArray(Aggregation[]::new);

            var importMetaData = ImmutableImportMetaData.builder()
                .projection(projection)
                .aggregations(aggregationsWithDefault)
                .propertyKeyIds(propertyKeyIds)
                .defaultValues(propertyDefaultValues)
                .typeTokenId(NO_SUCH_RELATIONSHIP_TYPE)
                .preAggregate(GdsFeatureToggles.USE_PRE_AGGREGATION.isEnabled())
                .build();

            var importerFactory = new SingleTypeRelationshipImporterFactoryBuilder()
                .importMetaData(importMetaData)
                .nodeCountSupplier(idMap::nodeCount)
                .importSizing(importSizing)
                .validateRelationships(cypherConfig.validateRelationships())
                .allocationTracker(loadingContext.allocationTracker())
                .build();

            return ImmutableRelationshipTypeImportContext
                .builder()
                .relationshipType(relationshipType)
                .relationshipProjection(projection)
                .singleTypeRelationshipImporterFactory(importerFactory)
                .build();
        }

    }

    @ValueClass
    interface LoadResult {
        GraphDimensions dimensions();

        RelationshipsAndProperties relationshipsAndProperties();
    }
}

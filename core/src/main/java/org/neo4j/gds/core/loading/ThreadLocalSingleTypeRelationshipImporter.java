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

import org.immutables.value.Value;
import org.jetbrains.annotations.NotNull;
import org.neo4j.gds.PropertyMapping;
import org.neo4j.gds.RelationshipProjection;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.core.Aggregation;
import org.neo4j.gds.core.compress.AdjacencyCompressorFactory;
import org.neo4j.gds.core.compress.AdjacencyListBehavior;
import org.neo4j.gds.core.compress.AdjacencyListsWithProperties;
import org.neo4j.gds.core.utils.mem.AllocationTracker;
import org.neo4j.kernel.api.KernelTransaction;

import java.util.Collection;
import java.util.Map;
import java.util.function.LongSupplier;

/**
 * Wraps a relationship buffer that is being filled by the store scanners.
 * Forwards the relationship buffer to the
 * {@link org.neo4j.gds.core.loading.RelationshipImporter}
 * which prepares the buffer content for consumption by the
 * {@link AdjacencyBuffer}.
 *
 * Each importing thread holds an instance of this class for each relationship
 * type that is being imported.
 */
@Value.Style(typeBuilder = "SingleTypeRelationshipImporterFactoryBuilder")
public final class ThreadLocalSingleTypeRelationshipImporter {

    private final RelationshipImporter.Imports imports;
    private final RelationshipImporter.PropertyReader propertyReader;
    private final RelationshipsBatchBuffer relationshipsBatchBuffer;

    private ThreadLocalSingleTypeRelationshipImporter(
        RelationshipImporter.Imports imports,
        RelationshipImporter.PropertyReader propertyReader,
        RelationshipsBatchBuffer relationshipsBatchBuffer
    ) {
        this.imports = imports;
        this.propertyReader = propertyReader;
        this.relationshipsBatchBuffer = relationshipsBatchBuffer;
    }

    public RelationshipsBatchBuffer buffer() {
        return relationshipsBatchBuffer;
    }

    public long importRelationships() {
        return imports.importRelationships(relationshipsBatchBuffer, propertyReader);
    }

    @org.immutables.builder.Builder.Factory
    public static Factory builder(
        ImportMetaData importMetaData,
        LongSupplier nodeCountSupplier,
        boolean validateRelationships,
        ImportSizing importSizing,
        AllocationTracker allocationTracker
    ) {
        var adjacencyCompressorFactory = AdjacencyListBehavior.asConfigured(
            nodeCountSupplier,
            importMetaData.projection().properties(),
            importMetaData.aggregations(),
            allocationTracker
        );

        var adjacencyBuilder = new AdjacencyBufferBuilder()
            .importMetaData(importMetaData)
            .importSizing(importSizing)
            .allocationTracker(allocationTracker)
            .adjacencyCompressorFactory(adjacencyCompressorFactory)
            .build();

        var relationshipImporter = new RelationshipImporter(adjacencyBuilder, allocationTracker);

        var projection = importMetaData.projection();
        var loadProperties = projection.properties().hasMappings();
        var imports = relationshipImporter.imports(projection.orientation(), loadProperties);

        return new Factory(
            adjacencyCompressorFactory,
            importMetaData.typeTokenId(),
            relationshipImporter,
            imports,
            validateRelationships,
            loadProperties
        );
    }

    public static final class Factory {

        private final AdjacencyCompressorFactory adjacencyCompressorFactory;
        private final int typeId;

        private final RelationshipImporter importer;
        private final RelationshipImporter.Imports imports;

        private final boolean validateRelationships;
        private final boolean loadProperties;

        private Factory(
            AdjacencyCompressorFactory adjacencyCompressorFactory,
            int typeToken,
            RelationshipImporter importer,
            RelationshipImporter.Imports imports,
            boolean validateRelationships,
            boolean loadProperties
        ) {
            this.adjacencyCompressorFactory = adjacencyCompressorFactory;
            this.typeId = typeToken;
            this.importer = importer;
            this.imports = imports;
            this.validateRelationships = validateRelationships;
            this.loadProperties = loadProperties;
        }

        public Collection<AdjacencyBuffer.AdjacencyListBuilderTask> adjacencyListBuilderTasks() {
            return importer.adjacencyListBuilderTasks();
        }

        public ThreadLocalSingleTypeRelationshipImporter createImporter(
            IdMap idMap,
            int bulkSize,
            RelationshipImporter.PropertyReader propertyReader
        ) {
            return new ThreadLocalSingleTypeRelationshipImporter(imports, propertyReader, createBuffer(idMap, bulkSize));
        }

        public AdjacencyListsWithProperties build() {
            return adjacencyCompressorFactory.build();
        }

        ThreadLocalSingleTypeRelationshipImporter createImporter(
            IdMap idMap,
            int bulkSize,
            KernelTransaction kernelTransaction
        ) {
            RelationshipImporter.PropertyReader propertyReader = loadProperties
                ? importer.storeBackedPropertiesReader(kernelTransaction)
                : (relationshipReferences, propertyReferences, numberOfReferences, propertyKeyIds, defaultValues, aggregations, atLeastOnePropertyToLoad) -> new long[propertyKeyIds.length][0];

            return new ThreadLocalSingleTypeRelationshipImporter(imports, propertyReader, createBuffer(idMap, bulkSize));
        }

        @NotNull
        private RelationshipsBatchBuffer createBuffer(IdMap idMap, int bulkSize) {
            return new RelationshipsBatchBuffer(
                idMap.cloneIdMap(),
                typeId,
                bulkSize,
                validateRelationships
            );
        }
    }

    @ValueClass
    public interface ImportMetaData {
        RelationshipProjection projection();

        Aggregation[] aggregations();

        int[] propertyKeyIds();

        double[] defaultValues();

        int typeTokenId();

        boolean preAggregate();

        static ImportMetaData of(RelationshipProjection projection, int typeTokenId, Map<String, Integer> relationshipPropertyTokens, boolean preAggregate) {
            return ImmutableImportMetaData
                .builder()
                .projection(projection)
                .aggregations(aggregations(projection))
                .propertyKeyIds(propertyKeyIds(projection, relationshipPropertyTokens))
                .defaultValues(defaultValues(projection))
                .typeTokenId(typeTokenId)
                .preAggregate(preAggregate)
                .build();
        }

        private static double[] defaultValues(RelationshipProjection projection) {
            return projection
                .properties()
                .mappings()
                .stream()
                .mapToDouble(propertyMapping -> propertyMapping.defaultValue().doubleValue())
                .toArray();
        }

        private static int[] propertyKeyIds(
            RelationshipProjection projection,
            Map<String, Integer> relationshipPropertyTokens
        ) {
            return projection.properties().mappings()
                .stream()
                .mapToInt(mapping -> relationshipPropertyTokens.get(mapping.neoPropertyKey())).toArray();
        }

        private static Aggregation[] aggregations(RelationshipProjection projection) {
            var propertyMappings = projection.properties().mappings();

            Aggregation[] aggregations = propertyMappings.stream()
                .map(PropertyMapping::aggregation)
                .map(Aggregation::resolve)
                .toArray(Aggregation[]::new);

            if (propertyMappings.isEmpty()) {
                aggregations = new Aggregation[]{Aggregation.resolve(projection.aggregation())};
            }

            return aggregations;
        }
    }

    @ValueClass
    public interface RelationshipTypeImportContext {
        RelationshipType relationshipType();

        RelationshipProjection relationshipProjection();

        ThreadLocalSingleTypeRelationshipImporter.Factory singleTypeRelationshipImporterFactory();
    }
}

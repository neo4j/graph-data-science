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
import org.neo4j.gds.api.PartialIdMap;
import org.neo4j.gds.api.compress.AdjacencyCompressor;
import org.neo4j.gds.api.compress.AdjacencyCompressorFactory;
import org.neo4j.gds.api.compress.AdjacencyListsWithProperties;
import org.neo4j.gds.core.Aggregation;
import org.neo4j.kernel.api.KernelTransaction;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.function.LongConsumer;
import java.util.function.LongSupplier;

@Value.Style(typeBuilder = "SingleTypeRelationshipImporterBuilder")
public final class SingleTypeRelationshipImporter {

    private final AdjacencyCompressorFactory adjacencyCompressorFactory;
    private final ImportMetaData importMetaData;
    private final int typeId;

    private final AdjacencyBuffer adjacencyBuffer;
    // TODO: move to importmetadata
    private final boolean skipDanglingRelationships;

    @org.immutables.builder.Builder.Factory
    public static SingleTypeRelationshipImporter of(
        ImportMetaData importMetaData,
        LongSupplier nodeCountSupplier,
        boolean skipDanglingRelationships,
        ImportSizing importSizing
    ) {
        var adjacencyCompressorFactory = AdjacencyListBehavior.asConfigured(
            nodeCountSupplier,
            importMetaData.projection().properties(),
            importMetaData.aggregations()
        );

        var adjacencyBuffer = new AdjacencyBufferBuilder()
            .importMetaData(importMetaData)
            .importSizing(importSizing)
            .adjacencyCompressorFactory(adjacencyCompressorFactory)
            .build();

        return new SingleTypeRelationshipImporter(
            adjacencyCompressorFactory,
            adjacencyBuffer,
            importMetaData,
            importMetaData.typeTokenId(),
            skipDanglingRelationships
        );
    }

    private SingleTypeRelationshipImporter(
        AdjacencyCompressorFactory adjacencyCompressorFactory,
        AdjacencyBuffer adjacencyBuffer,
        ImportMetaData importMetaData,
        int typeToken,
        boolean skipDanglingRelationships
    ) {
        this.adjacencyCompressorFactory = adjacencyCompressorFactory;
        this.importMetaData = importMetaData;
        this.typeId = typeToken;
        this.adjacencyBuffer = adjacencyBuffer;
        this.skipDanglingRelationships = skipDanglingRelationships;
    }

    public Collection<AdjacencyBuffer.AdjacencyListBuilderTask> adjacencyListBuilderTasks(Optional<AdjacencyCompressor.ValueMapper> mapper) {
        return adjacencyBuffer.adjacencyListBuilderTasks(mapper, Optional.empty());
    }

    public Collection<AdjacencyBuffer.AdjacencyListBuilderTask> adjacencyListBuilderTasks(
        Optional<AdjacencyCompressor.ValueMapper> mapper,
        Optional<LongConsumer> drainCountConsumer
    ) {
        return adjacencyBuffer.adjacencyListBuilderTasks(mapper, drainCountConsumer);
    }

    public ThreadLocalSingleTypeRelationshipImporter threadLocalImporter(
        PartialIdMap idMap,
        int bulkSize,
        PropertyReader propertyReader
    ) {
        return new ThreadLocalSingleTypeRelationshipImporterBuilder()
            .adjacencyBuffer(adjacencyBuffer)
            .relationshipsBatchBuffer(createBuffer(idMap, bulkSize))
            .importMetaData(importMetaData)
            .propertyReader(propertyReader)
            .build();
    }

    ThreadLocalSingleTypeRelationshipImporter threadLocalImporter(
        PartialIdMap idMap,
        int bulkSize,
        KernelTransaction kernelTransaction
    ) {
        var loadProperties = importMetaData.projection().properties().hasMappings();

        PropertyReader propertyReader = loadProperties
            ? PropertyReader.storeBacked(kernelTransaction)
            : (relationshipReferences, propertyReferences, numberOfReferences, propertyKeyIds, defaultValues, aggregations, atLeastOnePropertyToLoad) -> new long[propertyKeyIds.length][0];

        return new ThreadLocalSingleTypeRelationshipImporterBuilder()
            .adjacencyBuffer(adjacencyBuffer)
            .relationshipsBatchBuffer(createBuffer(idMap, bulkSize))
            .importMetaData(importMetaData)
            .propertyReader(propertyReader)
            .build();
    }

    @NotNull
    private RelationshipsBatchBuffer createBuffer(PartialIdMap idMap, int bulkSize) {
        return new RelationshipsBatchBuffer(
            idMap,
            typeId,
            bulkSize,
            skipDanglingRelationships
        );
    }

    public AdjacencyListsWithProperties build() {
        return adjacencyCompressorFactory.build();
    }

    @ValueClass
    public interface ImportMetaData {
        RelationshipProjection projection();

        Aggregation[] aggregations();

        int[] propertyKeyIds();

        double[] defaultValues();

        int typeTokenId();

        static ImportMetaData of(
            RelationshipProjection projection,
            int typeTokenId,
            Map<String, Integer> relationshipPropertyTokens
        ) {
            return ImmutableImportMetaData
                .builder()
                .projection(projection)
                .aggregations(aggregations(projection))
                .propertyKeyIds(propertyKeyIds(projection, relationshipPropertyTokens))
                .defaultValues(defaultValues(projection))
                .typeTokenId(typeTokenId)
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
    public interface SingleTypeRelationshipImportContext {
        RelationshipType relationshipType();

        Optional<RelationshipType> inverseOfRelationshipType();

        RelationshipProjection relationshipProjection();

        SingleTypeRelationshipImporter singleTypeRelationshipImporter();
    }
}

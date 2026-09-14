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

import org.neo4j.gds.Aggregation;
import org.neo4j.gds.PropertyMapping;
import org.neo4j.gds.RelationshipProjection;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.AdjacencyListsWithProperties;
import org.neo4j.gds.compression.api.AdjacencyCompressor;
import org.neo4j.gds.compression.api.AdjacencyCompressorFactory;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.function.LongConsumer;
import java.util.function.LongSupplier;

public final class SingleTypeRelationshipImporter {

    private final AdjacencyCompressorFactory adjacencyCompressorFactory;
    private final ImportMetaData importMetaData;
    private final int typeId;

    private final AdjacencyBuffer adjacencyBuffer;

    public static SingleTypeRelationshipImporter of(
        ImportMetaData importMetaData,
        LongSupplier nodeCountSupplier,
        ImportSizing importSizing
    ) {
        var compressorFactory = AdjacencyListBehavior.asConfigured(
            nodeCountSupplier,
            importMetaData.projection().properties(),
            importMetaData.aggregations()
        );
        return of(importMetaData, importSizing, compressorFactory);
    }

    public static SingleTypeRelationshipImporter of(
        ImportMetaData importMetaData,
        LongSupplier nodeCountSupplier,
        ImportSizing importSizing,
        AdjacencyListBehavior.Factory adjacencyCompressorFactoryFactory
    ) {
        var compressorFactory = adjacencyCompressorFactoryFactory.create(
            nodeCountSupplier,
            importMetaData.projection().properties(),
            importMetaData.aggregations()
        );
        return of(importMetaData, importSizing, compressorFactory);
    }

    public static SingleTypeRelationshipImporter of(
        ImportMetaData importMetaData,
        ImportSizing importSizing,
        AdjacencyCompressorFactory adjacencyCompressorFactory
    ) {
        var adjacencyBuffer = AdjacencyBuffer.of(importMetaData, adjacencyCompressorFactory, importSizing);

        return new SingleTypeRelationshipImporter(
            adjacencyCompressorFactory,
            adjacencyBuffer,
            importMetaData,
            importMetaData.typeTokenId()
        );
    }

    private SingleTypeRelationshipImporter(
        AdjacencyCompressorFactory adjacencyCompressorFactory,
        AdjacencyBuffer adjacencyBuffer,
        ImportMetaData importMetaData,
        int typeToken
    ) {
        this.adjacencyCompressorFactory = adjacencyCompressorFactory;
        this.importMetaData = importMetaData;
        this.typeId = typeToken;
        this.adjacencyBuffer = adjacencyBuffer;
    }

    public int typeId() {
        return this.typeId;
    }

    public boolean skipDanglingRelationships() {
        return this.importMetaData.skipDanglingRelationships();
    }

    public boolean loadProperties() {
        return this.importMetaData.projection().properties().hasMappings();
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

    public <PROPERTY_REF> ThreadLocalSingleTypeRelationshipImporter<PROPERTY_REF> threadLocalImporter(
        RelationshipsBatchBuffer<PROPERTY_REF> relationshipsBatchBuffer,
        PropertyReader<PROPERTY_REF> propertyReader
    ) {
        return ThreadLocalSingleTypeRelationshipImporter.of(
            adjacencyBuffer,
            relationshipsBatchBuffer,
            importMetaData,
            propertyReader
        );
    }

    public AdjacencyListsWithProperties build() {
        return adjacencyCompressorFactory.build(true);
    }

    public record ImportMetaData(
        RelationshipProjection projection,
        Aggregation[] aggregations,
        int[] propertyKeyIds,
        double[] defaultValues,
        int typeTokenId,
        boolean skipDanglingRelationships
    ) {

        public static ImportMetaData of(
            RelationshipProjection projection,
            int typeTokenId,
            Map<String, Integer> relationshipPropertyTokens,
            boolean skipDanglingRelationships
        ) {
            return new ImportMetaData(
                projection,
                aggregations(projection),
                propertyKeyIds(projection, relationshipPropertyTokens),
                defaultValues(projection),
                typeTokenId,
                skipDanglingRelationships
            );
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
                .mapToInt(mapping -> relationshipPropertyTokens.get(mapping.externalPropertyKey())).toArray();
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

    public record SingleTypeRelationshipImportContext(
        RelationshipType relationshipType,
        RelationshipProjection relationshipProjection,
        SingleTypeRelationshipImporter singleTypeRelationshipImporter,
        Optional<RelationshipType> inverseOfRelationshipType
    ) {
        public SingleTypeRelationshipImportContext(
            RelationshipType relationshipType,
            RelationshipProjection relationshipProjection,
            SingleTypeRelationshipImporter singleTypeRelationshipImporter
        ) {
            this(relationshipType, relationshipProjection, singleTypeRelationshipImporter, Optional.empty());
        }
        public static SingleTypeRelationshipImportContext of(RelationshipType rt, RelationshipProjection rp, SingleTypeRelationshipImporter stri) {
            return new SingleTypeRelationshipImportContext(rt, rp, stri);
        }
        public static SingleTypeRelationshipImportContext of(RelationshipType rt, RelationshipProjection rp, SingleTypeRelationshipImporter stri, RelationshipType irt) {
            return new SingleTypeRelationshipImportContext(rt, rp, stri, Optional.of(rt));
        }
    }
}

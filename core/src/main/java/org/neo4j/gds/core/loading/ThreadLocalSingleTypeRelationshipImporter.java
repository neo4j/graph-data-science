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

import org.immutables.builder.Builder;
import org.immutables.value.Value;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.core.utils.RawValues;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

/**
 * Wraps a relationship buffer that is being filled by the store scanners.
 *
 * Prepares the content of the relationship buffer for consumption by the
 * {@link AdjacencyBuffer}.
 *
 * Each importing thread holds an instance of this class for each relationship
 * type that is being imported.
 */
@Value.Style(typeBuilder = "ThreadLocalSingleTypeRelationshipImporterBuilder")
public abstract class ThreadLocalSingleTypeRelationshipImporter<PROPERTY_REF> {

    private final AdjacencyBuffer adjacencyBuffer;
    private final RelationshipsBatchBuffer<PROPERTY_REF> relationshipsBatchBuffer;

    final PropertyReader<PROPERTY_REF> propertyReader;

    @Builder.Factory
    static <PROPERTY_REF> ThreadLocalSingleTypeRelationshipImporter<PROPERTY_REF> of(
        AdjacencyBuffer adjacencyBuffer,
        RelationshipsBatchBuffer<PROPERTY_REF> relationshipsBatchBuffer,
        SingleTypeRelationshipImporter.ImportMetaData importMetaData,
        PropertyReader<PROPERTY_REF> propertyReader
    ) {
        var orientation = importMetaData.projection().orientation();
        var loadProperties = importMetaData.projection().properties().hasMappings();

        if (orientation == Orientation.UNDIRECTED) {
            return loadProperties
                ? new UndirectedWithProperties<>(
                adjacencyBuffer,
                relationshipsBatchBuffer,
                propertyReader
            )
                : new Undirected<>(adjacencyBuffer, relationshipsBatchBuffer, propertyReader);
        } else if (orientation == Orientation.NATURAL) {
            return loadProperties
                ? new NaturalWithProperties<>(
                adjacencyBuffer,
                relationshipsBatchBuffer,
                propertyReader
            )
                : new Natural<>(adjacencyBuffer, relationshipsBatchBuffer, propertyReader);
        } else if (orientation == Orientation.REVERSE) {
            return loadProperties
                ? new ReverseWithProperties<>(
                adjacencyBuffer,
                relationshipsBatchBuffer,
                propertyReader
            )
                : new Reverse<>(adjacencyBuffer, relationshipsBatchBuffer, propertyReader);
        } else {
            throw new IllegalArgumentException(formatWithLocale("Unexpected orientation: %s", orientation));
        }
    }

    private ThreadLocalSingleTypeRelationshipImporter(
        AdjacencyBuffer adjacencyBuffer,
        RelationshipsBatchBuffer<PROPERTY_REF> relationshipsBatchBuffer,
        PropertyReader<PROPERTY_REF> propertyReader
    ) {
        this.adjacencyBuffer = adjacencyBuffer;
        this.relationshipsBatchBuffer = relationshipsBatchBuffer;
        this.propertyReader = propertyReader;
    }

    public abstract long importRelationships();

    // TODO: remove, once Cypher loading uses RelationshipsBuilder
    public RelationshipsBatchBuffer<PROPERTY_REF> buffer() {
        return relationshipsBatchBuffer;
    }

    RelationshipsBatchBuffer<PROPERTY_REF> sourceBuffer() {
        return relationshipsBatchBuffer;
    }

    AdjacencyBuffer targetBuffer() {
        return adjacencyBuffer;
    }

    int importRelationships(
        RelationshipsBatchBuffer.View<PROPERTY_REF> sourceBuffer,
        long[][] properties,
        AdjacencyBuffer targetBuffer
    ) {
        long[] batch = sourceBuffer.batch();
        int batchLength = sourceBuffer.batchLength();
        int[] offsets = sourceBuffer.spareInts();
        long[] targets = sourceBuffer.spareLongs();

        long source, target, prevSource = batch[0];
        int offset = 0, nodesLength = 0;

        for (int i = 0; i < batchLength; i += 2) {
            source = batch[i];
            target = batch[1 + i];
            // If rels come in chunks for same source node
            // then we can skip adding an extra offset
            if (source > prevSource) {
                offsets[nodesLength++] = offset;
                prevSource = source;
            }
            targets[offset++] = target;
        }
        offsets[nodesLength++] = offset;

        targetBuffer.addAll(
            batch,
            targets,
            properties,
            offsets,
            nodesLength
        );

        return batchLength >> 1; // divide by 2
    }

    private static final class Undirected<PROPERTY_REF> extends ThreadLocalSingleTypeRelationshipImporter<PROPERTY_REF> {

        private Undirected(
            AdjacencyBuffer adjacencyBuffer,
            RelationshipsBatchBuffer<PROPERTY_REF> relationshipsBatchBuffer,
            PropertyReader<PROPERTY_REF> propertyReader
        ) {
            super(adjacencyBuffer, relationshipsBatchBuffer, propertyReader);
        }

        @Override
        public long importRelationships() {
            var bySource = sourceBuffer().changeToSourceOrder();
            int importedOut = importRelationships(bySource, null, targetBuffer());
            var byTarget = sourceBuffer().changeToTargetOrder();
            int importedIn = importRelationships(byTarget, null, targetBuffer());
            return RawValues.combineIntInt(importedOut + importedIn, 0);
        }
    }

    private static final class UndirectedWithProperties<PROPERTY_REF> extends ThreadLocalSingleTypeRelationshipImporter<PROPERTY_REF> {

        private UndirectedWithProperties(
            AdjacencyBuffer adjacencyBuffer,
            RelationshipsBatchBuffer<PROPERTY_REF> relationshipsBatchBuffer,
            PropertyReader<PROPERTY_REF> propertyReader
        ) {
            super(adjacencyBuffer, relationshipsBatchBuffer, propertyReader);
        }

        @Override
        public long importRelationships() {
            var bySource = sourceBuffer().changeToSourceOrder();
            long[][] outProperties = propertyReader.readProperties(
                bySource,
                targetBuffer().getPropertyKeyIds(),
                targetBuffer().getDefaultValues(),
                targetBuffer().getAggregations(),
                targetBuffer().atLeastOnePropertyToLoad()
            );
            int importedOut = importRelationships(bySource, outProperties, targetBuffer());

            var byTarget = sourceBuffer().changeToTargetOrder();
            long[][] inProperties = propertyReader.readProperties(
                byTarget,
                targetBuffer().getPropertyKeyIds(),
                targetBuffer().getDefaultValues(),
                targetBuffer().getAggregations(),
                targetBuffer().atLeastOnePropertyToLoad()
            );
            int importedIn = importRelationships(byTarget, inProperties, targetBuffer());

            return RawValues.combineIntInt(importedOut + importedIn, importedOut + importedIn);
        }
    }

    private static final class Natural<PROPERTY_REF> extends ThreadLocalSingleTypeRelationshipImporter<PROPERTY_REF> {

        private Natural(
            AdjacencyBuffer adjacencyBuffer,
            RelationshipsBatchBuffer<PROPERTY_REF> relationshipsBatchBuffer,
            PropertyReader<PROPERTY_REF> propertyReader
        ) {
            super(adjacencyBuffer, relationshipsBatchBuffer, propertyReader);
        }

        @Override
        public long importRelationships() {
            var bySource = sourceBuffer().changeToSourceOrder();
            return RawValues.combineIntInt(importRelationships(bySource, null, targetBuffer()), 0);
        }
    }

    private static final class NaturalWithProperties<PROPERTY_REF> extends ThreadLocalSingleTypeRelationshipImporter<PROPERTY_REF> {

        private NaturalWithProperties(
            AdjacencyBuffer adjacencyBuffer,
            RelationshipsBatchBuffer<PROPERTY_REF> relationshipsBatchBuffer,
            PropertyReader<PROPERTY_REF> propertyReader
        ) {
            super(adjacencyBuffer, relationshipsBatchBuffer, propertyReader);
        }

        @Override
        public long importRelationships() {
            var propertiesProducer = sourceBuffer().changeToSourceOrder();
            long[][] outProperties = propertyReader.readProperties(
                propertiesProducer,
                targetBuffer().getPropertyKeyIds(),
                targetBuffer().getDefaultValues(),
                targetBuffer().getAggregations(),
                targetBuffer().atLeastOnePropertyToLoad()
            );
            int importedOut = importRelationships(
                propertiesProducer,
                outProperties,
                targetBuffer()
            );
            return RawValues.combineIntInt(importedOut, importedOut);
        }
    }

    private static final class Reverse<PROPERTY_REF> extends ThreadLocalSingleTypeRelationshipImporter<PROPERTY_REF> {

        private Reverse(
            AdjacencyBuffer adjacencyBuffer,
            RelationshipsBatchBuffer<PROPERTY_REF> relationshipsBatchBuffer,
            PropertyReader<PROPERTY_REF> propertyReader
        ) {
            super(adjacencyBuffer, relationshipsBatchBuffer, propertyReader);
        }

        @Override
        public long importRelationships() {
            var byTarget = sourceBuffer().changeToTargetOrder();
            return RawValues.combineIntInt(importRelationships(byTarget, null, targetBuffer()), 0);
        }
    }

    private static final class ReverseWithProperties<PROPERTY_REF> extends ThreadLocalSingleTypeRelationshipImporter<PROPERTY_REF> {

        private ReverseWithProperties(
            AdjacencyBuffer adjacencyBuffer,
            RelationshipsBatchBuffer<PROPERTY_REF> relationshipsBatchBuffer,
            PropertyReader<PROPERTY_REF> propertyReader
        ) {
            super(adjacencyBuffer, relationshipsBatchBuffer, propertyReader);
        }

        @Override
        public long importRelationships() {
            var byTarget = sourceBuffer().changeToTargetOrder();
            long[][] inProperties = propertyReader.readProperties(
                byTarget,
                targetBuffer().getPropertyKeyIds(),
                targetBuffer().getDefaultValues(),
                targetBuffer().getAggregations(),
                targetBuffer().atLeastOnePropertyToLoad()
            );
            int importedIn = importRelationships(byTarget, inProperties, targetBuffer());
            return RawValues.combineIntInt(importedIn, importedIn);
        }
    }
}

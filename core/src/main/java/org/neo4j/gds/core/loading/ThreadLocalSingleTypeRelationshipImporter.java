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
public abstract class ThreadLocalSingleTypeRelationshipImporter {

    private final AdjacencyBuffer adjacencyBuffer;
    private final RelationshipsBatchBuffer relationshipsBatchBuffer;

    final PropertyReader propertyReader;

    @Builder.Factory
    static ThreadLocalSingleTypeRelationshipImporter of(
        AdjacencyBuffer adjacencyBuffer,
        RelationshipsBatchBuffer relationshipsBatchBuffer,
        SingleTypeRelationshipImporter.ImportMetaData importMetaData,
        PropertyReader propertyReader
    ) {
        var orientation = importMetaData.projection().orientation();
        var loadProperties = importMetaData.projection().properties().hasMappings();

        if (orientation == Orientation.UNDIRECTED) {
            return loadProperties
                ? new UndirectedWithProperties(
                adjacencyBuffer,
                relationshipsBatchBuffer,
                propertyReader
            )
                : new Undirected(adjacencyBuffer, relationshipsBatchBuffer, propertyReader);
        } else if (orientation == Orientation.NATURAL) {
            return loadProperties
                ? new NaturalWithProperties(
                adjacencyBuffer,
                relationshipsBatchBuffer,
                propertyReader
            )
                : new Natural(adjacencyBuffer, relationshipsBatchBuffer, propertyReader);
        } else if (orientation == Orientation.REVERSE) {
            return loadProperties
                ? new ReverseWithProperties(
                adjacencyBuffer,
                relationshipsBatchBuffer,
                propertyReader
            )
                : new Reverse(adjacencyBuffer, relationshipsBatchBuffer, propertyReader);
        } else {
            throw new IllegalArgumentException(formatWithLocale("Unexpected orientation: %s", orientation));
        }
    }

    private ThreadLocalSingleTypeRelationshipImporter(
        AdjacencyBuffer adjacencyBuffer,
        RelationshipsBatchBuffer relationshipsBatchBuffer,
        PropertyReader propertyReader
    ) {
        this.adjacencyBuffer = adjacencyBuffer;
        this.relationshipsBatchBuffer = relationshipsBatchBuffer;
        this.propertyReader = propertyReader;
    }

    public abstract long importRelationships();

    // TODO: remove, once Cypher loading uses RelationshipsBuilder
    public RelationshipsBatchBuffer buffer() {
        return relationshipsBatchBuffer;
    }

    protected RelationshipsBatchBuffer sourceBuffer() {
        return relationshipsBatchBuffer;
    }

    protected AdjacencyBuffer targetBuffer() {
        return adjacencyBuffer;
    }

    protected int importRelationships(
        RelationshipsBatchBuffer sourceBuffer,
        long[] batch,
        long[][] properties,
        AdjacencyBuffer targetBuffer
    ) {
        int batchLength = sourceBuffer.length();

        // TODO: to we need to pass sourceBuffer in addition?
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

    private static final class Undirected extends ThreadLocalSingleTypeRelationshipImporter {

        private Undirected(
            AdjacencyBuffer adjacencyBuffer,
            RelationshipsBatchBuffer relationshipsBatchBuffer,
            PropertyReader propertyReader
        ) {
            super(adjacencyBuffer, relationshipsBatchBuffer, propertyReader);
        }

        @Override
        public long importRelationships() {
            long[] batch = sourceBuffer().sortBySource();
            int importedOut = importRelationships(sourceBuffer(), batch, null, targetBuffer());
            batch = sourceBuffer().sortByTarget();
            int importedIn = importRelationships(sourceBuffer(), batch, null, targetBuffer());
            return RawValues.combineIntInt(importedOut + importedIn, 0);
        }
    }

    private static final class UndirectedWithProperties extends ThreadLocalSingleTypeRelationshipImporter {

        private UndirectedWithProperties(
            AdjacencyBuffer adjacencyBuffer,
            RelationshipsBatchBuffer relationshipsBatchBuffer,
            PropertyReader propertyReader
        ) {
            super(adjacencyBuffer, relationshipsBatchBuffer, propertyReader);
        }

        @Override
        public long importRelationships() {
            int batchLength = sourceBuffer().length();
            long[] batch = sourceBuffer().sortBySource();
            long[][] outProperties = propertyReader.readProperty(
                sourceBuffer().relationshipReferences(),
                sourceBuffer().propertyReferences(),
                batchLength / RelationshipsBatchBuffer.ENTRIES_PER_RELATIONSHIP,
                targetBuffer().getPropertyKeyIds(),
                targetBuffer().getDefaultValues(),
                targetBuffer().getAggregations(),
                targetBuffer().atLeastOnePropertyToLoad()
            );
            int importedOut = importRelationships(sourceBuffer(), batch, outProperties, targetBuffer());

            batch = sourceBuffer().sortByTarget();
            long[][] inProperties = propertyReader.readProperty(
                sourceBuffer().relationshipReferences(),
                sourceBuffer().propertyReferences(),
                batchLength / RelationshipsBatchBuffer.ENTRIES_PER_RELATIONSHIP,
                targetBuffer().getPropertyKeyIds(),
                targetBuffer().getDefaultValues(),
                targetBuffer().getAggregations(),
                targetBuffer().atLeastOnePropertyToLoad()
            );
            int importedIn = importRelationships(sourceBuffer(), batch, inProperties, targetBuffer());

            return RawValues.combineIntInt(importedOut + importedIn, importedOut + importedIn);
        }
    }

    private static final class Natural extends ThreadLocalSingleTypeRelationshipImporter {

        private Natural(
            AdjacencyBuffer adjacencyBuffer,
            RelationshipsBatchBuffer relationshipsBatchBuffer,
            PropertyReader propertyReader
        ) {
            super(adjacencyBuffer, relationshipsBatchBuffer, propertyReader);
        }

        @Override
        public long importRelationships() {
            long[] batch = sourceBuffer().sortBySource();
            return RawValues.combineIntInt(importRelationships(
                sourceBuffer(),
                batch,
                null,
                targetBuffer()
            ), 0);
        }
    }

    private static final class NaturalWithProperties extends ThreadLocalSingleTypeRelationshipImporter {

        private NaturalWithProperties(
            AdjacencyBuffer adjacencyBuffer,
            RelationshipsBatchBuffer relationshipsBatchBuffer,
            PropertyReader propertyReader
        ) {
            super(adjacencyBuffer, relationshipsBatchBuffer, propertyReader);
        }

        @Override
        public long importRelationships() {
            int batchLength = sourceBuffer().length();
            long[] batch = sourceBuffer().sortBySource();
            long[][] outProperties = propertyReader.readProperty(
                sourceBuffer().relationshipReferences(),
                sourceBuffer().propertyReferences(),
                batchLength / 2,
                targetBuffer().getPropertyKeyIds(),
                targetBuffer().getDefaultValues(),
                targetBuffer().getAggregations(),
                targetBuffer().atLeastOnePropertyToLoad()
            );
            int importedOut = importRelationships(sourceBuffer(), batch, outProperties, targetBuffer());
            return RawValues.combineIntInt(importedOut, importedOut);
        }
    }

    private static final class Reverse extends ThreadLocalSingleTypeRelationshipImporter {

        private Reverse(
            AdjacencyBuffer adjacencyBuffer,
            RelationshipsBatchBuffer relationshipsBatchBuffer,
            PropertyReader propertyReader
        ) {
            super(adjacencyBuffer, relationshipsBatchBuffer, propertyReader);
        }

        @Override
        public long importRelationships() {
            long[] batch = sourceBuffer().sortByTarget();
            return RawValues.combineIntInt(importRelationships(
                sourceBuffer(),
                batch,
                null,
                targetBuffer()
            ), 0);
        }
    }

    private static final class ReverseWithProperties extends ThreadLocalSingleTypeRelationshipImporter {

        private ReverseWithProperties(
            AdjacencyBuffer adjacencyBuffer,
            RelationshipsBatchBuffer relationshipsBatchBuffer,
            PropertyReader propertyReader
        ) {
            super(adjacencyBuffer, relationshipsBatchBuffer, propertyReader);
        }

        @Override
        public long importRelationships() {
            int batchLength = sourceBuffer().length();
            long[] batch = sourceBuffer().sortByTarget();
            long[][] inProperties = propertyReader.readProperty(
                sourceBuffer().relationshipReferences(),
                sourceBuffer().propertyReferences(),
                batchLength / 2,
                targetBuffer().getPropertyKeyIds(),
                targetBuffer().getDefaultValues(),
                targetBuffer().getAggregations(),
                targetBuffer().atLeastOnePropertyToLoad()
            );
            int importedIn = importRelationships(sourceBuffer(), batch, inProperties, targetBuffer());
            return RawValues.combineIntInt(importedIn, importedIn);
        }
    }
}

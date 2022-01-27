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
import org.neo4j.gds.core.utils.mem.AllocationTracker;

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
public final class ThreadLocalSingleTypeRelationshipImporter {

    private final AdjacencyBuffer adjacencyBuffer;
    private final ImportStrategy importStrategy;
    private final RelationshipsBatchBuffer relationshipsBatchBuffer;

    @Builder.Factory
    static ThreadLocalSingleTypeRelationshipImporter of(
        AdjacencyBuffer adjacencyBuffer,
        RelationshipsBatchBuffer relationshipsBatchBuffer,
        SingleTypeRelationshipImporter.ImportMetaData importMetaData,
        PropertyReader propertyReader,
        AllocationTracker allocationTracker
    ) {
        var importStrategy = ImportStrategy.of(importMetaData, propertyReader, allocationTracker);
        return new ThreadLocalSingleTypeRelationshipImporter(adjacencyBuffer, importStrategy, relationshipsBatchBuffer);
    }

    private ThreadLocalSingleTypeRelationshipImporter(
        AdjacencyBuffer adjacencyBuffer,
        ImportStrategy importStrategy,
        RelationshipsBatchBuffer relationshipsBatchBuffer
    ) {
        this.adjacencyBuffer = adjacencyBuffer;
        this.importStrategy = importStrategy;
        this.relationshipsBatchBuffer = relationshipsBatchBuffer;
    }

    public RelationshipsBatchBuffer buffer() {
        return relationshipsBatchBuffer;
    }

    public long importRelationships() {
        return importStrategy.importRelationships(relationshipsBatchBuffer, adjacencyBuffer);
    }

    public abstract static class ImportStrategy {
        protected final PropertyReader propertyReader;
        private final AllocationTracker allocationTracker;

        static ImportStrategy of(
            SingleTypeRelationshipImporter.ImportMetaData importMetaData,
            PropertyReader propertyReader,
            AllocationTracker allocationTracker
        ) {
            var orientation = importMetaData.projection().orientation();
            var loadProperties = importMetaData.projection().properties().hasMappings();

            if (orientation == Orientation.UNDIRECTED) {
                return loadProperties
                    ? new UndirectedWithPropertiesStrategy(propertyReader, allocationTracker)
                    : new UndirectedStrategy(propertyReader, allocationTracker);
            } else if (orientation == Orientation.NATURAL) {
                return loadProperties
                    ? new NaturalWithPropertiesStrategy(propertyReader, allocationTracker)
                    : new NaturalStrategy(propertyReader, allocationTracker);
            } else if (orientation == Orientation.REVERSE) {
                return loadProperties
                    ? new ReverseWithPropertiesStrategy(propertyReader, allocationTracker)
                    : new ReverseStrategy(propertyReader, allocationTracker);
            } else {
                throw new IllegalArgumentException(formatWithLocale("Unexpected orientation: %s", orientation));
            }
        }

        protected ImportStrategy(
            PropertyReader propertyReader,
            AllocationTracker allocationTracker
        ) {
            this.propertyReader = propertyReader;
            this.allocationTracker = allocationTracker;
        }

        abstract long importRelationships(
            RelationshipsBatchBuffer sourceBuffer,
            AdjacencyBuffer targetBuffer
        );

        int importRelationships(
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
                nodesLength,
                allocationTracker
            );

            return batchLength >> 1; // divide by 2
        }
    }

    private static final class UndirectedStrategy extends ImportStrategy {

        private UndirectedStrategy(PropertyReader propertyReader, AllocationTracker allocationTracker) {
            super(propertyReader, allocationTracker);
        }

        @Override
        public long importRelationships(RelationshipsBatchBuffer sourceBuffer, AdjacencyBuffer targetBuffer) {
            long[] batch = sourceBuffer.sortBySource();
            int importedOut = importRelationships(sourceBuffer, batch, null, targetBuffer);
            batch = sourceBuffer.sortByTarget();
            int importedIn = importRelationships(sourceBuffer, batch, null, targetBuffer);
            return RawValues.combineIntInt(importedOut + importedIn, 0);
        }
    }

    private static final class UndirectedWithPropertiesStrategy extends ImportStrategy {

        UndirectedWithPropertiesStrategy(PropertyReader propertyReader, AllocationTracker allocationTracker) {
            super(propertyReader, allocationTracker);
        }

        @Override
        long importRelationships(RelationshipsBatchBuffer sourceBuffer, AdjacencyBuffer targetBuffer) {
            int batchLength = sourceBuffer.length();
            long[] batch = sourceBuffer.sortBySource();
            long[][] outProperties = propertyReader.readProperty(
                sourceBuffer.relationshipReferences(),
                sourceBuffer.propertyReferences(),
                batchLength / 2,
                targetBuffer.getPropertyKeyIds(),
                targetBuffer.getDefaultValues(),
                targetBuffer.getAggregations(),
                targetBuffer.atLeastOnePropertyToLoad()
            );
            int importedOut = importRelationships(sourceBuffer, batch, outProperties, targetBuffer);

            batch = sourceBuffer.sortByTarget();
            long[][] inProperties = propertyReader.readProperty(
                sourceBuffer.relationshipReferences(),
                sourceBuffer.propertyReferences(),
                batchLength / 2,
                targetBuffer.getPropertyKeyIds(),
                targetBuffer.getDefaultValues(),
                targetBuffer.getAggregations(),
                targetBuffer.atLeastOnePropertyToLoad()
            );
            int importedIn = importRelationships(sourceBuffer, batch, inProperties, targetBuffer);

            return RawValues.combineIntInt(importedOut + importedIn, importedOut + importedIn);

        }
    }


    static final class NaturalStrategy extends ImportStrategy {
        NaturalStrategy(PropertyReader propertyReader, AllocationTracker allocationTracker) {
            super(propertyReader, allocationTracker);
        }

        @Override
        long importRelationships(RelationshipsBatchBuffer sourceBuffer, AdjacencyBuffer targetBuffer) {
            long[] batch = sourceBuffer.sortBySource();
            return RawValues.combineIntInt(importRelationships(sourceBuffer, batch, null, targetBuffer), 0);
        }
    }

    static final class NaturalWithPropertiesStrategy extends ImportStrategy {

        NaturalWithPropertiesStrategy(PropertyReader propertyReader, AllocationTracker allocationTracker) {
            super(propertyReader, allocationTracker);
        }

        @Override
        long importRelationships(RelationshipsBatchBuffer sourceBuffer, AdjacencyBuffer targetBuffer) {
            int batchLength = sourceBuffer.length();
            long[] batch = sourceBuffer.sortBySource();
            long[][] outProperties = propertyReader.readProperty(
                sourceBuffer.relationshipReferences(),
                sourceBuffer.propertyReferences(),
                batchLength / 2,
                targetBuffer.getPropertyKeyIds(),
                targetBuffer.getDefaultValues(),
                targetBuffer.getAggregations(),
                targetBuffer.atLeastOnePropertyToLoad()
            );
            int importedOut = importRelationships(sourceBuffer, batch, outProperties, targetBuffer);
            return RawValues.combineIntInt(importedOut, importedOut);
        }
    }

    static final class ReverseStrategy extends ImportStrategy {

        protected ReverseStrategy(PropertyReader propertyReader, AllocationTracker allocationTracker) {
            super(propertyReader, allocationTracker);
        }

        @Override
        long importRelationships(RelationshipsBatchBuffer sourceBuffer, AdjacencyBuffer targetBuffer) {
            long[] batch = sourceBuffer.sortByTarget();
            return RawValues.combineIntInt(importRelationships(sourceBuffer, batch, null, targetBuffer), 0);
        }
    }


    static final class ReverseWithPropertiesStrategy extends ImportStrategy {

        ReverseWithPropertiesStrategy(PropertyReader propertyReader, AllocationTracker allocationTracker) {
            super(propertyReader, allocationTracker);
        }

        @Override
        long importRelationships(RelationshipsBatchBuffer sourceBuffer, AdjacencyBuffer targetBuffer) {
            int batchLength = sourceBuffer.length();
            long[] batch = sourceBuffer.sortByTarget();
            long[][] inProperties = propertyReader.readProperty(
                sourceBuffer.relationshipReferences(),
                sourceBuffer.propertyReferences(),
                batchLength / 2,
                targetBuffer.getPropertyKeyIds(),
                targetBuffer.getDefaultValues(),
                targetBuffer.getAggregations(),
                targetBuffer.atLeastOnePropertyToLoad()
            );
            int importedIn = importRelationships(sourceBuffer, batch, inProperties, targetBuffer);
            return RawValues.combineIntInt(importedIn, importedIn);

        }
    }
}

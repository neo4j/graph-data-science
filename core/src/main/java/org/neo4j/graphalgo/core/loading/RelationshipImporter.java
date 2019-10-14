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

import org.neo4j.graphalgo.core.utils.RawValues;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.internal.kernel.api.CursorFactory;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.Read;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

public class RelationshipImporter {

    private final AllocationTracker tracker;
    private final AdjacencyBuilder outAdjacency;
    private final AdjacencyBuilder inAdjacency;

    public RelationshipImporter(
            AllocationTracker tracker,
            AdjacencyBuilder outAdjacency,
            AdjacencyBuilder inAdjacency) {
        this.tracker = tracker;
        this.outAdjacency = outAdjacency;
        this.inAdjacency = inAdjacency;
    }

    public interface Imports {
        long importRels(
                RelationshipsBatchBuffer batches,
                PropertyReader propertyReader);
    }

    public Imports imports(
            boolean loadAsUndirected,
            boolean loadOutgoing,
            boolean loadIncoming,
            boolean loadProperties) {
        if (loadAsUndirected) {
            return loadProperties
                    ? this::importBothOrUndirectedWithProperties
                    : this::importBothOrUndirected;
        }
        if (loadOutgoing) {
            if (loadIncoming) {
                return loadProperties
                        ? this::importBothOrUndirectedWithProperties
                        : this::importBothOrUndirected;
            }
            return loadProperties
                    ? this::importOutgoingWithProperties
                    : this::importOutgoing;
        }
        if (loadIncoming) {
            return loadProperties
                    ? this::importIncomingWithProperties
                    : this::importIncoming;
        }
        return null;
    }

    private long importBothOrUndirected(RelationshipsBatchBuffer buffer, PropertyReader propertyReader) {
        long[] batch = buffer.sortBySource();
        int importedOut = importRelationships(buffer, batch, null, outAdjacency, tracker);
        batch = buffer.sortByTarget();
        int importedIn = importRelationships(buffer, batch, null, inAdjacency, tracker);
        return RawValues.combineIntInt(importedOut + importedIn, 0);
    }

    private long importBothOrUndirectedWithProperties(RelationshipsBatchBuffer buffer, PropertyReader reader) {
        int batchLength = buffer.length;
        long[] batch = buffer.sortBySource();
        long[][] outProperties = reader.readProperty(
                batch,
                batchLength,
                outAdjacency.getPropertyKeyIds(),
                outAdjacency.getDefaultValues());
        int importedOut = importRelationships(buffer, batch, outProperties, outAdjacency, tracker);
        batch = buffer.sortByTarget();

        long[][] inProperties = reader.readProperty(
                batch,
                batchLength,
                inAdjacency.getPropertyKeyIds(),
                inAdjacency.getDefaultValues());
        int importedIn = importRelationships(buffer, batch, inProperties, inAdjacency, tracker);
        return RawValues.combineIntInt(importedOut + importedIn, importedOut + importedIn);
    }

    private long importOutgoing(RelationshipsBatchBuffer buffer, PropertyReader propertyReader) {
        long[] batch = buffer.sortBySource();
        return RawValues.combineIntInt(importRelationships(buffer, batch, null, outAdjacency, tracker), 0);
    }

    private long importOutgoingWithProperties(RelationshipsBatchBuffer buffer, PropertyReader propertyReader) {
        int batchLength = buffer.length;
        long[] batch = buffer.sortBySource();
        long[][] outProperties = propertyReader.readProperty(
                batch,
                batchLength,
                outAdjacency.getPropertyKeyIds(),
                outAdjacency.getDefaultValues());
        int importedOut = importRelationships(buffer, batch, outProperties, outAdjacency, tracker);
        return RawValues.combineIntInt(importedOut, importedOut);
    }

    private long importIncoming(RelationshipsBatchBuffer buffer, PropertyReader propertyReader) {
        long[] batch = buffer.sortByTarget();
        return RawValues.combineIntInt(importRelationships(buffer, batch, null, inAdjacency, tracker), 0);
    }

    private long importIncomingWithProperties(RelationshipsBatchBuffer buffer, PropertyReader propertyReader) {
        int batchLength = buffer.length;
        long[] batch = buffer.sortByTarget();
        long[][] inProperties = propertyReader.readProperty(
                batch,
                batchLength,
                inAdjacency.getPropertyKeyIds(),
                inAdjacency.getDefaultValues());
        int importedIn = importRelationships(buffer, batch, inProperties, inAdjacency, tracker);
        return RawValues.combineIntInt(importedIn, importedIn);
    }

    public interface PropertyReader {
        long[][] readProperty(long[] batch, int batchLength, int[] propertyKeyIds, double[] defaultValues);
    }

    public Collection<Runnable> flushTasks() {
        if (outAdjacency != null) {
            if (inAdjacency == null || inAdjacency == outAdjacency) {
                return outAdjacency.flushTasks();
            }
            Collection<Runnable> tasks = new ArrayList<>(outAdjacency.flushTasks());
            tasks.addAll(inAdjacency.flushTasks());
            return tasks;
        }
        if (inAdjacency != null) {
            return inAdjacency.flushTasks();
        }
        return Collections.emptyList();
    }

    private static final int BATCH_ENTRY_SIZE = 4;
    private static final int RELATIONSHIP_REFERENCE_OFFSET = 2;
    private static final int PROPERTIES_REFERENCE_OFFSET = 3;

    PropertyReader storeBackedPropertiesReader(CursorFactory cursors, Read read) {
        return (batch, batchLength, relationshipProperties, defaultPropertyValues) -> {
            long[][] properties = new long[relationshipProperties.length][batchLength / BATCH_ENTRY_SIZE];
            try (PropertyCursor pc = cursors.allocatePropertyCursor()) {
                for (int i = 0; i < batchLength; i += BATCH_ENTRY_SIZE) {
                    long relationshipReference = batch[RELATIONSHIP_REFERENCE_OFFSET + i];
                    long propertiesReference = batch[PROPERTIES_REFERENCE_OFFSET + i];
                    read.relationshipProperties(relationshipReference, propertiesReference, pc);
                    double[] relProps = ReadHelper.readProperties(pc, relationshipProperties, defaultPropertyValues);
                    int propertyPos = i / BATCH_ENTRY_SIZE;
                    for (int j = 0; j < relProps.length; j++) {
                        properties[j][propertyPos] = Double.doubleToLongBits(relProps[j]);
                    }
                }
            }
            return properties;
        };
    }


    public static PropertyReader preLoadedPropertyReader() {
        return (batch, batchLength, weightProperty, defaultWeight) -> {
            long[] properties = new long[batchLength / BATCH_ENTRY_SIZE];
            for (int i = 0; i < batchLength; i += BATCH_ENTRY_SIZE) {
                long property = batch[PROPERTIES_REFERENCE_OFFSET + i];
                properties[i / BATCH_ENTRY_SIZE] = property;
            }
            return new long[][]{properties};
        };
    }

    private static int importRelationships(
            RelationshipsBatchBuffer buffer,
            long[] batch,
            long[][] properties,
            AdjacencyBuilder adjacency,
            AllocationTracker tracker
    ) {
        int batchLength = buffer.length;

        int[] offsets = buffer.spareInts();
        long[] targets = buffer.spareLongs();

        long source, target, prevSource = batch[0];
        int offset = 0, nodesLength = 0;

        for (int i = 0; i < batchLength; i += 4) {
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

        adjacency.addAll(
                batch,
                targets,
                properties,
                offsets,
                nodesLength,
                tracker
        );

        return batchLength >> 2; // divide by 4
    }
}

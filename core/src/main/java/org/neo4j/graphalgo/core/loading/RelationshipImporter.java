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
                WeightReader weightReader);
    }

    public Imports imports(
            boolean loadAsUndirected,
            boolean loadOutgoing,
            boolean loadIncoming,
            boolean loadWeights) {
        if (loadAsUndirected) {
            return loadWeights
                    ? this::importBothOrUndirectedWithWeight
                    : this::importBothOrUndirected;
        }
        if (loadOutgoing) {
            if (loadIncoming) {
                return loadWeights
                        ? this::importBothOrUndirectedWithWeight
                        : this::importBothOrUndirected;
            }
            return loadWeights
                    ? this::importOutgoingWithWeight
                    : this::importOutgoing;
        }
        if (loadIncoming) {
            return loadWeights
                    ? this::importIncomingWithWeight
                    : this::importIncoming;
        }
        return null;
    }

    private long importBothOrUndirected(RelationshipsBatchBuffer buffer, WeightReader weightReader) {
        long[] batch = buffer.sortBySource();
        int importedOut = importRelationships(buffer, batch, null, outAdjacency, tracker);
        batch = buffer.sortByTarget();
        int importedIn = importRelationships(buffer, batch, null, inAdjacency, tracker);
        return RawValues.combineIntInt(importedOut + importedIn, 0);
    }

    private long importBothOrUndirectedWithWeight(RelationshipsBatchBuffer buffer, WeightReader reader) {
        int batchLength = buffer.length;
        long[] batch = buffer.sortBySource();
        long[][] weightsOut = reader.readWeight(
                batch,
                batchLength,
                outAdjacency.getWeightProperties(),
                outAdjacency.getDefaultWeights());
        int importedOut = importRelationships(buffer, batch, weightsOut, outAdjacency, tracker);
        batch = buffer.sortByTarget();

        long[][] weightsIn = reader.readWeight(
                batch,
                batchLength,
                inAdjacency.getWeightProperties(),
                inAdjacency.getDefaultWeights());
        int importedIn = importRelationships(buffer, batch, weightsIn, inAdjacency, tracker);
        return RawValues.combineIntInt(importedOut + importedIn, importedOut + importedIn);
    }

    private long importOutgoing(RelationshipsBatchBuffer buffer, WeightReader weightReader) {
        long[] batch = buffer.sortBySource();
        return RawValues.combineIntInt(importRelationships(buffer, batch, null, outAdjacency, tracker), 0);
    }

    private long importOutgoingWithWeight(RelationshipsBatchBuffer buffer, WeightReader weightReader) {
        int batchLength = buffer.length;
        long[] batch = buffer.sortBySource();
        long[][] weightsOut = weightReader.readWeight(
                batch,
                batchLength,
                outAdjacency.getWeightProperties(),
                outAdjacency.getDefaultWeights());
        int importedOut = importRelationships(buffer, batch, weightsOut, outAdjacency, tracker);
        return RawValues.combineIntInt(importedOut, importedOut);
    }

    private long importIncoming(RelationshipsBatchBuffer buffer, WeightReader weightReader) {
        long[] batch = buffer.sortByTarget();
        return RawValues.combineIntInt(importRelationships(buffer, batch, null, inAdjacency, tracker), 0);
    }

    private long importIncomingWithWeight(RelationshipsBatchBuffer buffer, WeightReader weightReader) {
        int batchLength = buffer.length;
        long[] batch = buffer.sortByTarget();
        long[][] weightsIn = weightReader.readWeight(
                batch,
                batchLength,
                inAdjacency.getWeightProperties(),
                inAdjacency.getDefaultWeights());
        int importedIn = importRelationships(buffer, batch, weightsIn, inAdjacency, tracker);
        return RawValues.combineIntInt(importedIn, importedIn);
    }

    public interface WeightReader {
        long[][] readWeight(long[] batch, int batchLength, int[] weightProperty, double[] defaultWeight);
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

    WeightReader storeBackedWeightReader(CursorFactory cursors, Read read) {
        return (batch, batchLength, weightProperties, defaultWeights) -> {
            long[][] weights = new long[weightProperties.length][batchLength / BATCH_ENTRY_SIZE];
            try (PropertyCursor pc = cursors.allocatePropertyCursor()) {
                for (int i = 0; i < batchLength; i += BATCH_ENTRY_SIZE) {
                    long relationshipReference = batch[RELATIONSHIP_REFERENCE_OFFSET + i];
                    long propertiesReference = batch[PROPERTIES_REFERENCE_OFFSET + i];
                    read.relationshipProperties(relationshipReference, propertiesReference, pc);
                    double[] relWeights = ReadHelper.readProperties(pc, weightProperties, defaultWeights);
                    int weightPos = i / BATCH_ENTRY_SIZE;
                    for (int j = 0; j < relWeights.length; j++) {
                        weights[j][weightPos] = Double.doubleToLongBits(relWeights[j]);
                    }
                }
            }
            return weights;
        };
    }


    public static WeightReader preLoadedWeightReader() {
        return (batch, batchLength, weightProperty, defaultWeight) -> {
            long[] weights = new long[batchLength / BATCH_ENTRY_SIZE];
            for (int i = 0; i < batchLength; i += BATCH_ENTRY_SIZE) {
                long weight = batch[PROPERTIES_REFERENCE_OFFSET + i];
                weights[i / BATCH_ENTRY_SIZE] = weight;
            }
            return new long[][]{weights};
        };
    }

    private static int importRelationships(
            RelationshipsBatchBuffer buffer,
            long[] batch,
            long[][] weights,
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
                weights,
                offsets,
                nodesLength,
                tracker
        );

        return batchLength >> 2; // divide by 4
    }
}
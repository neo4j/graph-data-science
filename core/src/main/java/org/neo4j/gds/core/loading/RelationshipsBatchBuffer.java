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

import java.lang.reflect.Array;

public final class RelationshipsBatchBuffer<PROPERTY_REF> extends RecordsBatchBuffer {

    // For relationships, the buffer is divided into 2-long blocks
    // for each relationship: source, target. Relationship and
    // property references are stored individually.
    private static final int ENTRIES_PER_RELATIONSHIP = 2;

    private final long[] relationshipReferences;
    private final PROPERTY_REF[] propertyReferences;

    private final long[] bufferCopy;
    private final long[] relationshipReferencesCopy;
    private final PROPERTY_REF[] propertyReferencesCopy;
    private final int[] histogram;

    @Builder.Factory
    static <PROPERTY_REF> RelationshipsBatchBuffer<PROPERTY_REF> relationshipsBatchBuffer(
        int capacity,
        Class<PROPERTY_REF> propertyReferenceClass
    ) {
        return new RelationshipsBatchBuffer<>(capacity, propertyReferenceClass);
    }

    private RelationshipsBatchBuffer(int capacity, Class<PROPERTY_REF> propertyReferenceClass) {
        super(Math.multiplyExact(ENTRIES_PER_RELATIONSHIP, capacity));
        this.relationshipReferences = new long[capacity];
        //noinspection unchecked
        this.propertyReferences = (PROPERTY_REF[]) Array.newInstance(propertyReferenceClass, capacity);
        bufferCopy = RadixSort.newCopy(buffer);
        relationshipReferencesCopy = RadixSort.newCopy(relationshipReferences);
        propertyReferencesCopy = RadixSort.newCopy(propertyReferences);
        histogram = RadixSort.newHistogram(capacity);
    }

    public void add(long sourceId, long targetId) {
        int position = this.length;
        long[] buffer = this.buffer;
        buffer[position] = sourceId;
        buffer[1 + position] = targetId;
        this.length = 2 + position;
    }

    public void add(long sourceId, long targetId, long relationshipReference, PROPERTY_REF propertyReference) {
        int position = this.length;
        long[] buffer = this.buffer;
        buffer[position] = sourceId;
        buffer[1 + position] = targetId;
        this.relationshipReferences[position >> 1] = relationshipReference;
        this.propertyReferences[position >> 1] = propertyReference;
        this.length = 2 + position;
    }

    View<PROPERTY_REF> changeToSourceOrder() {
        sortBySource();
        return new View<>(
            this.buffer,
            this.length,
            this.relationshipReferences,
            this.propertyReferences,
            this.bufferCopy,
            this.histogram
        );
    }

    View<PROPERTY_REF> changeToTargetOrder() {
        sortByTarget();
        return new View<>(
            this.buffer,
            this.length,
            this.relationshipReferences,
            this.propertyReferences,
            this.bufferCopy,
            this.histogram
        );
    }

    private void sortBySource() {
        RadixSort.radixSort(
            buffer,
            bufferCopy,
            relationshipReferences,
            relationshipReferencesCopy,
            propertyReferences,
            propertyReferencesCopy,
            histogram,
            length
        );
    }

    private void sortByTarget() {
        RadixSort.radixSort2(
            buffer,
            bufferCopy,
            relationshipReferences,
            relationshipReferencesCopy,
            propertyReferences,
            propertyReferencesCopy,
            histogram,
            length
        );
    }

    public static final class View<PROPERTY_REF> implements PropertyReader.Producer<PROPERTY_REF> {
        private final long[] nodePairs;
        private final int nodePairsLength;

        private final long[] relationshipReferences;
        private final PROPERTY_REF[] propertyReferences;

        private final long[] spareLongs;
        private final int[] spareInts;


        private View(
            long[] nodePairs,
            int nodePairsLength,
            long[] relationshipReferences,
            PROPERTY_REF[] propertyReferences,
            long[] spareLongs,
            int[] spareInts
        ) {
            this.nodePairs = nodePairs;
            this.nodePairsLength = nodePairsLength;
            this.relationshipReferences = relationshipReferences;
            this.propertyReferences = propertyReferences;
            this.spareLongs = spareLongs;
            this.spareInts = spareInts;
        }

        @Override
        public int numberOfElements() {
            return this.nodePairsLength / ENTRIES_PER_RELATIONSHIP;
        }

        @Override
        public void forEach(PropertyReader.Consumer<PROPERTY_REF> consumer) {
            int length = this.numberOfElements();
            for (int i = 0; i < length; i++) {
                consumer.accept(
                    i,
                    this.nodePairs[i << 1],
                    this.nodePairs[(i << 1) + 1],
                    this.relationshipReferences[i],
                    this.propertyReferences[i]
                );
            }
        }

        int[] spareInts() {
            return this.spareInts;
        }

        long[] spareLongs() {
            return this.spareLongs;
        }

        long[] batch() {
            return this.nodePairs;
        }

        int batchLength() {
            return this.nodePairsLength;
        }
    }
}

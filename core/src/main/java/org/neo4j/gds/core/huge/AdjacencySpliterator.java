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
package org.neo4j.gds.core.huge;

import org.neo4j.gds.api.AdjacencyCursor;
import org.neo4j.gds.api.PropertyCursor;
import org.neo4j.gds.api.RelationshipCursor;
import org.neo4j.gds.api.ModifiableRelationshipCursor;

import java.util.Spliterator;
import java.util.function.Consumer;

abstract class AdjacencySpliterator implements Spliterator<RelationshipCursor> {
    private final AdjacencyCursor adjacencyCursor;
    private final ModifiableRelationshipCursor modifiableRelationshipCursor;

    static Spliterator<RelationshipCursor> of(
        AdjacencyCursor adjacencyCursor,
        long sourceNodeId,
        double fallbackValue
    ) {
        return new WithoutProperty(adjacencyCursor, sourceNodeId, fallbackValue);
    }

    static Spliterator<RelationshipCursor> of(
        AdjacencyCursor adjacencyCursor,
        PropertyCursor propertyCursor,
        long sourceNodeId
    ) {
        return new WithProperty(adjacencyCursor, propertyCursor, sourceNodeId);
    }

    private AdjacencySpliterator(
        AdjacencyCursor adjacencyCursor,
        long sourceNodeId
    ) {
        this.adjacencyCursor = adjacencyCursor;
        this.modifiableRelationshipCursor = ModifiableRelationshipCursor.create().setSourceId(sourceNodeId);
    }

    @Override
    public boolean tryAdvance(Consumer<? super RelationshipCursor> action) {
        if (adjacencyCursor.hasNextVLong()) {
            modifyCursor(modifiableRelationshipCursor.setTargetId(adjacencyCursor.nextVLong()));
            // We need to pass a modifiable cursor. This is important for downstream
            // dependencies, such as NodeFilteredGraph#streamRelationships.
            action.accept(modifiableRelationshipCursor);
            return true;
        }
        return false;
    }

    @Override
    public Spliterator<RelationshipCursor> trySplit() {
        return null;
    }

    @Override
    public long estimateSize() {
        return adjacencyCursor.remaining();
    }

    @Override
    public int characteristics() {
        return Spliterator.ORDERED | Spliterator.DISTINCT | Spliterator.SIZED;
    }

    abstract void modifyCursor(ModifiableRelationshipCursor cursor);

    static final class WithoutProperty extends AdjacencySpliterator {

        private final double fallbackValue;

        WithoutProperty(AdjacencyCursor adjacencyCursor, long sourceNodeId, double fallbackValue) {
            super(adjacencyCursor, sourceNodeId);
            this.fallbackValue = fallbackValue;
        }

        @Override
        void modifyCursor(ModifiableRelationshipCursor cursor) {
            cursor.setProperty(fallbackValue);
        }
    }

    static final class WithProperty extends AdjacencySpliterator {

        private final PropertyCursor propertyCursor;

        WithProperty(
            AdjacencyCursor adjacencyCursor,
            PropertyCursor propertyCursor,
            long sourceNodeId
        ) {
            super(adjacencyCursor, sourceNodeId);
            this.propertyCursor = propertyCursor;
        }

        @Override
        void modifyCursor(ModifiableRelationshipCursor cursor) {
            long propertyBits = propertyCursor.nextLong();
            double property = Double.longBitsToDouble(propertyBits);
            cursor.setProperty(property);
        }
    }
}

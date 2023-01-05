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

import org.jetbrains.annotations.Nullable;
import org.neo4j.gds.api.AdjacencyCursor;
import org.neo4j.gds.api.AdjacencyList;
import org.neo4j.gds.api.AdjacencyProperties;
import org.neo4j.gds.api.CompositeRelationshipIterator;
import org.neo4j.gds.api.PropertyCursor;

import java.util.Optional;

public class CSRCompositeRelationshipIterator implements CompositeRelationshipIterator {

    public static final AdjacencyProperties[] EMPTY_PROPERTIES = new AdjacencyProperties[0];

    private final String[] propertyKeys;

    private final AdjacencyList adjacencyList;
    @Nullable
    private final AdjacencyList inverseAdjacencyList;
    private final AdjacencyProperties[] properties;
    private final double[] propertyBuffer;
    private final AdjacencyProperties[] inverseProperties;

    private final AdjacencyCursor adjacencyCursor;
    private final AdjacencyCursor inverseAdjacencyCursor;
    private final PropertyCursor[] propertyCursors;
    private final PropertyCursor[] inversePropertyCursors;

    public CSRCompositeRelationshipIterator(
        AdjacencyList adjacencyList,
        Optional<AdjacencyList> inverseAdjacencyList,
        String[] propertyKeys,
        AdjacencyProperties[] properties,
        AdjacencyProperties[] inverseProperties
    ) {
        var propertyCount = propertyKeys.length;

        assert properties.length == propertyCount;

        this.adjacencyList = adjacencyList;
        this.inverseAdjacencyList = inverseAdjacencyList.orElse(null);
        this.propertyKeys = propertyKeys;
        this.properties = properties;
        this.inverseProperties = inverseProperties;

        this.propertyBuffer = new double[propertyCount];
        this.adjacencyCursor = adjacencyList.rawAdjacencyCursor();
        this.inverseAdjacencyCursor = inverseAdjacencyList.map(AdjacencyList::rawAdjacencyCursor).orElse(null);

        this.propertyCursors = new PropertyCursor[propertyCount];
        for (int i = 0; i < propertyCount; i++) {
            this.propertyCursors[i] = properties[i].rawPropertyCursor();
        }
        this.inversePropertyCursors = new PropertyCursor[inverseProperties.length];
        for (int i = 0; i < inverseProperties.length; i++) {
            this.inversePropertyCursors[i] = inverseProperties[i].rawPropertyCursor();
        }
    }

    @Override
    public int degree(long nodeId) {
        return adjacencyList.degree(nodeId);
    }

    @Override
    public void forEachRelationship(long nodeId, RelationshipConsumer consumer) {
        forEachRelationship(nodeId, consumer, adjacencyList, properties, adjacencyCursor, propertyCursors);
    }

    @Override
    public void forEachInverseRelationship(long nodeId, RelationshipConsumer consumer) {
        if (inverseAdjacencyList == null) {
            throw new UnsupportedOperationException(
                "Cannot create composite iterator on a relationship type that is not inverse indexed");
        }
        forEachRelationship(
            nodeId,
            consumer,
            inverseAdjacencyList,
            inverseProperties,
            inverseAdjacencyCursor,
            inversePropertyCursors
        );
    }

    private void forEachRelationship(
        long nodeId,
        RelationshipConsumer consumer,
        AdjacencyList adjacency,
        AdjacencyProperties[] props,
        AdjacencyCursor reuseAdjacencyCursor,
        PropertyCursor[] reusePropertyCursors
    ) {
        // init adjacency cursor
        adjacency.adjacencyCursor(reuseAdjacencyCursor, nodeId);
        if (!reuseAdjacencyCursor.hasNextVLong()) {
            return;
        }

        var propertyCount = propertyKeys.length;
        for (int propertyIdx = 0; propertyIdx < propertyCount; propertyIdx++) {
            props[propertyIdx].propertyCursor(reusePropertyCursors[propertyIdx], nodeId);
        }

        while (reuseAdjacencyCursor.hasNextVLong()) {
            var target = reuseAdjacencyCursor.nextVLong();
            for (int propertyIdx = 0; propertyIdx < propertyCount; propertyIdx++) {
                propertyBuffer[propertyIdx] = Double.longBitsToDouble(reusePropertyCursors[propertyIdx].nextLong());
            }

            if (!consumer.consume(nodeId, target, propertyBuffer)) {
                break;
            }
        }
    }

    @Override
    public String[] propertyKeys() {
        return propertyKeys;
    }

    @Override
    public CompositeRelationshipIterator concurrentCopy() {
        return new CSRCompositeRelationshipIterator(
            adjacencyList,
            Optional.ofNullable(inverseAdjacencyList),
            propertyKeys,
            properties,
            inverseProperties
        );
    }
}

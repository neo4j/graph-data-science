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

public class CSRCompositeRelationshipIterator implements CompositeRelationshipIterator {

    public static final AdjacencyProperties[] EMPTY_PROPERTIES = new AdjacencyProperties[0];

    private final AdjacencyList adjacencyList;
    @Nullable private final AdjacencyList inverseAdjacencyList;
    private final String[] propertyKeys;
    private final AdjacencyProperties[] properties;
    private final double[] propertyBuffer;
    @Nullable private final AdjacencyProperties[] inverseProperties;

    private AdjacencyCursor topologyCursor;
    private final PropertyCursor[] propertyCursors;

    public CSRCompositeRelationshipIterator(
        AdjacencyList adjacencyList,
        @Nullable AdjacencyList inverseAdjacencyList,
        String[] propertyKeys,
        AdjacencyProperties[] properties,
        @Nullable AdjacencyProperties[] inverseProperties
    ) {
        var propertyCount = propertyKeys.length;

        assert properties.length == propertyCount;

        this.adjacencyList = adjacencyList;
        this.inverseAdjacencyList = inverseAdjacencyList;
        this.propertyKeys = propertyKeys;
        this.properties = properties;
        this.inverseProperties = inverseProperties;

        this.propertyBuffer = new double[propertyCount];
        this.topologyCursor = AdjacencyCursor.empty();

        this.propertyCursors = new PropertyCursor[propertyCount];
        for (int i = 0; i < propertyCount; i++) {
            this.propertyCursors[i] = PropertyCursor.empty();
        }
    }

    @Override
    public int degree(long nodeId) {
        return adjacencyList.degree(nodeId);
    }

    @Override
    public void forEachRelationship(long nodeId, RelationshipConsumer consumer) {
        forEachRelationship(nodeId, consumer, adjacencyList, properties);
    }

    @Override
    public void forEachInverseRelationship(long nodeId, RelationshipConsumer consumer) {
        if (inverseAdjacencyList == null) {
            throw new UnsupportedOperationException(
                "Cannot create composite iterator on a relationship type that is not inverse indexed"
            );
        }
        forEachRelationship(nodeId, consumer, inverseAdjacencyList, inverseProperties);
    }

    private void forEachRelationship(
        long nodeId,
        RelationshipConsumer consumer,
        AdjacencyList adjacency,
        AdjacencyProperties[] props
    ) {
        // init adjacency cursor
        var adjacencyCursor = adjacency.adjacencyCursor(topologyCursor, nodeId);
        if (!adjacencyCursor.hasNextVLong()) {
            return;
        }

        topologyCursor = adjacencyCursor;
        var propertyCount = propertyKeys.length;
        for (int propertyIdx = 0; propertyIdx < propertyCount; propertyIdx++) {
            propertyCursors[propertyIdx] = props[propertyIdx].propertyCursor(nodeId);
        }

        while (adjacencyCursor.hasNextVLong()) {
            var target = adjacencyCursor.nextVLong();
            for (int propertyIdx = 0; propertyIdx < propertyCount; propertyIdx++) {
                propertyBuffer[propertyIdx] = Double.longBitsToDouble(propertyCursors[propertyIdx].nextLong());
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
            inverseAdjacencyList,
            propertyKeys,
            properties,
            inverseProperties
        );
    }
}

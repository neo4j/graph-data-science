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
package org.neo4j.graphalgo.core.huge;

import org.neo4j.graphalgo.api.AdjacencyCursor;
import org.neo4j.graphalgo.api.CompositeRelationshipIterator;
import org.neo4j.graphalgo.api.PropertyCursor;
import org.neo4j.graphalgo.api.Relationships;

public class CSRCompositeRelationshipIterator implements CompositeRelationshipIterator {

    private final Relationships.Topology topology;
    private final String[] propertyKeys;
    private final Relationships.Properties[] properties;
    private final double[] propertyBuffer;

    private final AdjacencyCursor topologyCursor;
    private final PropertyCursor[] propertyCursors;

    public CSRCompositeRelationshipIterator(
        Relationships.Topology topology,
        String[] propertyKeys,
        Relationships.Properties[] properties
    ) {
        var propertyCount = propertyKeys.length;

        assert properties.length == propertyCount;

        this.topology = topology;
        this.propertyKeys = propertyKeys;
        this.properties = properties;

        this.propertyBuffer = new double[propertyCount];
        this.topologyCursor = topology.list().rawDecompressingCursor();

        this.propertyCursors = new PropertyCursor[propertyCount];
        for (int i = 0; i < propertyCount; i++) {
            this.propertyCursors[i] = properties[i].list().rawCursor();
        }
    }

    public void forEachRelationship(long nodeId, RelationshipConsumer consumer) {
        var offset = topology.offsets().get(nodeId);

        if (offset == 0L) {
            return;
        }

        // init adjacency cursor
        var degree = topology.degrees().degree(nodeId);
        var adjacencyCursor = topologyCursor.initializedTo(offset, degree);


        var propertyCount = propertyKeys.length;

        for (int propertyIdx = 0; propertyIdx < propertyCount; propertyIdx++) {
            var propertyOffset = properties[propertyIdx].offsets().get(nodeId);
            propertyCursors[propertyIdx].init(propertyOffset, degree);
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
            topology,
            propertyKeys,
            properties
        );
    }
}

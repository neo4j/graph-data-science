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
package org.neo4j.gds.values;

import org.neo4j.gds.api.properties.nodes.DoubleNodePropertyValues;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

public class Neo4jDoubleNodePropertyValues implements DoubleNodePropertyValues, Neo4jNodePropertyValues {

    private final NodePropertyValues internal;

    public Neo4jDoubleNodePropertyValues(NodePropertyValues internal) {
        this.internal = internal;
    }

    @Override
    public Value value(long nodeId) {
        return neo4jValue(nodeId);
    }

    @Override
    public Value neo4jValue(long nodeId) {
        var value = doubleValue(nodeId);
        return Double.isNaN(value) ? null : Values.doubleValue(value);
    }

    @Override
    public double doubleValue(long nodeId) {
        return internal.doubleValue(nodeId);
    }

    @Override
    public long nodeCount() {
        return internal.nodeCount();
    }
}

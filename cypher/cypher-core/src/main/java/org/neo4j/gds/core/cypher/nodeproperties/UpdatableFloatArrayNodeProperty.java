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
package org.neo4j.gds.core.cypher.nodeproperties;

import org.neo4j.gds.api.properties.nodes.FloatArrayNodePropertyValues;
import org.neo4j.gds.collections.HugeSparseFloatArrayList;
import org.neo4j.gds.core.cypher.UpdatableNodeProperty;
import org.neo4j.gds.utils.Neo4jValueConversion;
import org.neo4j.values.storable.Value;

public class UpdatableFloatArrayNodeProperty implements UpdatableNodeProperty, FloatArrayNodePropertyValues {

    private final long nodeCount;
    private final HugeSparseFloatArrayList floatArrayList;

    public UpdatableFloatArrayNodeProperty(long nodeCount, float[] defaultValue) {
        this.nodeCount = nodeCount;
        this.floatArrayList = HugeSparseFloatArrayList.of(defaultValue);
    }

    @Override
    public long nodeCount() {
        return nodeCount;
    }

    @Override
    public float[] floatArrayValue(long nodeId) {
        return floatArrayList.get(nodeId);
    }

    @Override
    public void updatePropertyValue(long nodeId, Value value) {
        floatArrayList.set(nodeId, Neo4jValueConversion.getFloatArray(value));
    }

}

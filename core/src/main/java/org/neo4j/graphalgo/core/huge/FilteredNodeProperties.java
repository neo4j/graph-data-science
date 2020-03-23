/*
 * Copyright (c) 2017-2020 "Neo4j,"
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

import org.apache.commons.lang3.mutable.MutableDouble;
import org.neo4j.graphalgo.api.NodeProperties;
import org.neo4j.graphalgo.core.loading.IdMap;

import java.util.OptionalLong;

public class FilteredNodeProperties implements NodeProperties {
    private final NodeProperties properties;
    private IdMap idMap;

    FilteredNodeProperties(NodeProperties properties, IdMap idMap) {
        this.properties = properties;
        this.idMap = idMap;
    }

    @Override
    public double nodeProperty(long nodeId) {
        return properties.nodeProperty(idMap.toOriginalNodeId(nodeId));
    }

    @Override
    public double nodeProperty(long nodeId, double defaultValue) {
        return properties.nodeProperty(idMap.toOriginalNodeId(nodeId), defaultValue);
    }

    @Override
    public OptionalLong getMaxPropertyValue() {
        MutableDouble currentMax = new MutableDouble(Double.NEGATIVE_INFINITY);
        idMap.forEachNode(id -> {
            currentMax.setValue(Math.max(currentMax.doubleValue(), nodeProperty(id, Double.MIN_VALUE)));
            return true;
        });
        return currentMax.doubleValue() == Double.NEGATIVE_INFINITY
            ? OptionalLong.empty()
            : OptionalLong.of((long) currentMax.doubleValue());
    }

    @Override
    public long release() {
        long releasedFromProps = properties.release();
        idMap = null;
        return releasedFromProps;
    }

    @Override
    public long size() {
        return Math.max(properties.size(), idMap.nodeCount());
    }
}

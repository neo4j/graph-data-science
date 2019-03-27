/*
 * Copyright (c) 2017 "Neo4j, Inc." <http://neo4j.com>
 *
 * This file is part of Neo4j Graph Algorithms <http://github.com/neo4j-contrib/neo4j-graph-algorithms>.
 *
 * Neo4j Graph Algorithms is free software: you can redistribute it and/or modify
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
package org.neo4j.graphalgo.core.huge.loader;

import org.neo4j.graphalgo.api.HugeWeightMapping;
import org.neo4j.graphalgo.core.utils.paged.PagedLongDoubleMap;

final class HugeNodePropertyMap implements HugeWeightMapping {

    private PagedLongDoubleMap properties;
    private final double defaultValue;
    private final int propertyId;

    HugeNodePropertyMap(PagedLongDoubleMap properties, double defaultValue, int propertyId) {
        this.properties = properties;
        this.defaultValue = defaultValue;
        this.propertyId = propertyId;
    }

    @Override
    public double weight(final long source, final long target) {
        assert target == -1L;
        return properties.getOrDefault(source, defaultValue);
    }

    @Override
    public double weight(final long source, final long target, final double defaultValue) {
        assert target == -1L;
        return properties.getOrDefault(source, defaultValue);
    }

    public double defaultValue() {
        return defaultValue;
    }

    public void put(long nodeId, double value) {
        properties.put(nodeId, value);
    }

    @Override
    public long release() {
        if (properties != null) {
            long freed = properties.release();
            properties = null;
            return freed;
        }
        return 0L;
    }

    public int propertyId() {
        return propertyId;
    }
}

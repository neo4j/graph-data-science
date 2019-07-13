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
package org.neo4j.graphalgo.core.huge.loader;

import org.neo4j.graphalgo.api.HugeWeightMapping;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimation;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimations;
import org.neo4j.graphalgo.core.utils.paged.PagedLongDoubleMap;

final class HugeNodePropertyMap implements HugeWeightMapping {

    private static final MemoryEstimation MEMORY_ESTIMATION = MemoryEstimations
            .builder(HugeNodePropertyMap.class)
            .add("properties", PagedLongDoubleMap.memoryEstimation())
            .build();

    private PagedLongDoubleMap properties;
    private final double defaultValue;
    private final int propertyId;

    static MemoryEstimation memoryEstimation() {
        return MEMORY_ESTIMATION;
    }

    HugeNodePropertyMap(PagedLongDoubleMap properties, double defaultValue, int propertyId) {
        this.properties = properties;
        this.defaultValue = defaultValue;
        this.propertyId = propertyId;
    }

    @Override
    public long size() {
        return properties.size();
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

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
package org.neo4j.graphalgo.api.nodeproperties;

import org.neo4j.graphalgo.api.NodeProperties;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import java.util.OptionalLong;
import java.util.stream.LongStream;

@FunctionalInterface
public interface LongNodeProperties extends NodeProperties {

    @Override
    long getLong(long nodeId);

    @Override
    default Object getObject(long nodeId) {
        return getLong(nodeId);
    }

    @Override
    default Object getObject(long nodeId, Object defaultValue) {
        return getLong(nodeId, (Long) defaultValue);
    }

    @Override
    default Value getValue(long nodeId) {
        return Values.longValue(getLong(nodeId));
    };

    @Override
    default ValueType getType() {
        return ValueType.LONG;
    };

    @Override
    default double getDouble(long nodeId) {
        return getLong(nodeId);
    };

    @Override
    default OptionalLong getMaxLongPropertyValue() {
        return LongStream
            .range(0, size())
            .parallel()
            .map(this::getLong)
            .max();
    }
}

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
package org.neo4j.graphalgo.api;

import org.neo4j.graphalgo.api.nodeproperties.ValueType;
import org.neo4j.values.storable.Value;

import java.util.OptionalLong;

public interface NodeProperties {

    default double getDouble(long nodeId) {
        throw new UnsupportedOperationException("double is not supported");
    }

    default long getLong(long nodeId) {
        throw new UnsupportedOperationException("long is not supported");
    };

    default double[] getDoubleArray(long nodeId) {
        throw new UnsupportedOperationException("double[] is not supported");
    }

    default long[] getLongArray(long nodeId) {
        throw new UnsupportedOperationException("long[] is not supported");
    }

    default Object getObject(long nodeId) {
        return getDouble(nodeId);
    }

    ValueType getType();

    Value getValue(long nodeId);

    default double getDouble(long nodeId, double defaultValue) {
        return getDouble(nodeId);
    }

    default long getLong(long nodeId, long defaultValue) {
        return getLong(nodeId);
    }

    default double[] getDoubleArray(long nodeId, double[] defaultValue) {
        return getDoubleArray(nodeId);
    }

    default long[] getLongArray(long nodeId, long[] defaultValue) {
        return getLongArray(nodeId);
    }

    default Object getObject(long nodeId, Object defaultValue) {
        return getObject(nodeId);
    }

    /**
     * Release internal data structures and return an estimate how many bytes were freed.
     *
     * Note that the mapping is not usable afterwards.
     */
    default long release() {
        return 0;
    }

    /**
     * @return the number of values stored.
     */
    default long size() {
        return 0;
    }

    /**
     * @return the maximum value contained in the mapping or an empty {@link OptionalLong} if the mapping is
     *         empty or the feature is not supported.
     */
    @Deprecated
    default OptionalLong getMaxPropertyValue() {
        return OptionalLong.empty();
    }

}

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

import java.util.OptionalLong;

@FunctionalInterface
public interface NodeProperties {

    /**
     * Returns the property value for a node or the loaded default value if no property has been defined.
     */
    double nodeProperty(long nodeId);

    /**
     * Returns the property value for a node or the given default value if no property had been defined.
     * The default value has precedence over the default value defined by the loader.
     */
    default double nodeProperty(long nodeId, double defaultValue) {
        return nodeProperty(nodeId);
    }

    /**
     * @return the maximum value contained in the mapping or an empty {@link OptionalLong} if the mapping is
     *         empty or the feature is not supported.
     */
    default OptionalLong getMaxPropertyValue() {
        return OptionalLong.empty();
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
}

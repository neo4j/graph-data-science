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
package org.neo4j.graphalgo.api;

import org.neo4j.graphalgo.core.utils.RawValues;

/**
 * @author mknobloch
 */
public interface WeightMapping {

    /**
     * Returns the number of keys stored in that mapping.
     */
    long size();

    /**
     * returns the weight for ID if set or the load-time specified default weight otherwise
     */
    double get(long id);

    /**
     * returns the weight for ID if set or the given default weight otherwise
     */
    double get(long id, double defaultValue);

    default double get(int source, int target) {
        return get(RawValues.combineIntInt(source, target));
    }

    default double get(int id) {
        return get(RawValues.combineIntInt(id, -1));
    }

    default double get(int id, double defaultValue) {
        return get(RawValues.combineIntInt(id, -1), defaultValue);
    }

    default long getMaxValue() {
        return -1L;
    }
}

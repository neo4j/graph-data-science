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
package org.neo4j.gds.collections;

import com.carrotsearch.hppc.LongLongHashMap;

import java.util.Arrays;

public class LongMultiSet {

    private final LongLongHashMap map;

    public LongMultiSet() {map = new LongLongHashMap();}

    public long add(long value) {
        return map.addTo(value, 1);
    }

    public long add(long key, long count) {
        return map.addTo(key, count);
    }

    public long count(long value) {
        return map.getOrDefault(value, 0);
    }

    public long[] keys() {
        return map.keys().toArray();
    }

    public long size() {
        return map.size();
    }
    public long sum() { return Arrays.stream(map.values).sum(); }
}

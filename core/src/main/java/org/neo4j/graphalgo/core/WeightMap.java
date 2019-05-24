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
package org.neo4j.graphalgo.core;

import com.carrotsearch.hppc.LongDoubleHashMap;
import com.carrotsearch.hppc.LongDoubleMap;
import org.neo4j.graphalgo.api.WeightMapping;

/**
 * single weight cache
 */
public final class WeightMap implements WeightMapping {

    private final int capacity;
    private LongDoubleMap weights;
    private final double defaultValue;
    private final int propertyId;

    public WeightMap(
            int capacity,
            double defaultValue,
            int propertyId) {
        this.capacity = capacity;
        this.defaultValue = defaultValue;
        this.weights = new LongDoubleHashMap();
        this.propertyId = propertyId;
    }

    public WeightMap(
            int capacity,
            LongDoubleMap weights,
            double defaultValue,
            int propertyId) {
        this.capacity = capacity;
        this.weights = weights;
        this.defaultValue = defaultValue;
        this.propertyId = propertyId;
    }

    /**
     * return the weight for id or defaultValue if unknown
     */
    @Override
    public double get(long id) {
        return weights.getOrDefault(id, defaultValue);
    }

    @Override
    public double get(final long id, final double defaultValue) {
        return weights.getOrDefault(id, defaultValue);
    }

    public void put(long key, double value) {
        weights.put(key, value);
    }

    /**
     * return the capacity
     */
    int capacity() {
        return capacity;
    }

    /**
     * return primitive map for the weights
     */
    public LongDoubleMap weights() {
        return weights;
    }

    public int propertyId() {
        return propertyId;
    }

    public int size() {
        return weights.size();
    }

    public double defaultValue() {
        return defaultValue;
    }
}

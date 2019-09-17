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

import org.neo4j.graphalgo.api.WeightMapping;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimation;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimations;

/**
 * WeightMapping implementation which always returns
 * a given default weight upon invocation
 *
 * @author mknblch
 */
public class NullWeightMap implements WeightMapping {

    static final MemoryEstimation MEMORY_USAGE = MemoryEstimations.of(NullWeightMap.class);

    private final double defaultValue;

    public NullWeightMap(double defaultValue) {
        this.defaultValue = defaultValue;
    }

    @Override
    public long size() {
        return 0;
    }

    @Override
    public double weight(final long source, final long target) {
        return defaultValue;
    }

    @Override
    public double weight(final long source, final long target, final double defaultValue) {
        return defaultValue;
    }

    @Override
    public double nodeWeight(final long nodeId) {
        return defaultValue;
    }

    @Override
    public double nodeWeight(final long nodeId, final double defaultValue) {
        return defaultValue;
    }

    @Override
    public double weight(final long id) {
        return defaultValue;
    }

    @Override
    public double weight(final long id, final double defaultValue) {
        return defaultValue;
    }

    @Override
    public double weight(final int source, final int target) {
        return defaultValue;
    }

    @Override
    public double nodeWeight(final int id) {
        return defaultValue;
    }

    @Override
    public double nodeWeight(final int id, final double defaultValue) {
        return defaultValue;
    }

    @Override
    public long release() {
        return 0L;
    }
}

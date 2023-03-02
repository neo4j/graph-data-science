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
package org.neo4j.gds.leiden;

import org.neo4j.gds.api.properties.nodes.LongArrayNodePropertyValues;

import java.util.function.LongFunction;

final class IntermediateCommunityNodeProperties implements LongArrayNodePropertyValues {

    private final long maxIndex;
    private final long storedValues;
    private final LongFunction<long[]> intermediateCommunity;

    IntermediateCommunityNodeProperties(long maxIndex, long storedValues, LongFunction<long[]> intermediateCommunity) {
        this.maxIndex = maxIndex;
        this.storedValues = storedValues;
        this.intermediateCommunity = intermediateCommunity;
    }

    @Override
    public long valuesStored() {
        return storedValues;
    }

    @Override
    public long maxIndex() {
        return maxIndex;
    }

    @Override
    public long[] longArrayValue(long nodeId) {
        return intermediateCommunity.apply(nodeId);
    }
}

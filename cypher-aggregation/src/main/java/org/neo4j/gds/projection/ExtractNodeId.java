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
package org.neo4j.gds.projection;

import org.neo4j.values.AnyValue;
import org.neo4j.values.SequenceValue;
import org.neo4j.values.storable.IntegralValue;
import org.neo4j.values.virtual.VirtualNodeValue;

import java.util.concurrent.atomic.AtomicBoolean;

final class ExtractNodeId implements PartialValueMapper<Long> {
    private final AtomicBoolean hasSeenArbitraryIds = new AtomicBoolean(false);

    @Override
    public Long unsupported(AnyValue value) {
        throw invalidNodeType(value.getTypeName());
    }

    @Override
    public Long mapSequence(SequenceValue value) {
        throw invalidNodeType("List");
    }

    @Override
    public Long mapNode(VirtualNodeValue value) {
        return value.id();
    }

    @Override
    public Long mapIntegral(IntegralValue value) {
        this.hasSeenArbitraryIds.lazySet(true);
        return value.longValue();
    }

    private static IllegalArgumentException invalidNodeType(String typeName) {
        return new IllegalArgumentException("The node has to be either a NODE or an INTEGER, but got " + typeName);
    }

    boolean hasSeenArbitraryIds() {
        return hasSeenArbitraryIds.get();
    }
}

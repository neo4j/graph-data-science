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
package org.neo4j.gds.nodeproperties;

import org.eclipse.collections.api.block.function.primitive.LongToObjectFunction;
import org.neo4j.gds.api.properties.nodes.LongArrayNodePropertyValues;

public final class LongArrayTestPropertyValues implements LongArrayNodePropertyValues {
    private final LongToObjectFunction<long[]> transformer;

    public LongArrayTestPropertyValues(LongToObjectFunction<long[]> transformer) {this.transformer = transformer;}

    @Override
    public long valuesStored() {
        return 0;
    }

    @Override
    public long[] longArrayValue(long nodeId) {
        return transformer.apply(nodeId);
    }
}

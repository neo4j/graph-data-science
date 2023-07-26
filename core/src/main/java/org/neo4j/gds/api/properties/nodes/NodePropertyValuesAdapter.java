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
package org.neo4j.gds.api.properties.nodes;

import org.neo4j.gds.collections.haa.HugeAtomicDoubleArray;
import org.neo4j.gds.collections.haa.HugeAtomicLongArray;
import org.neo4j.gds.core.utils.paged.HugeByteArray;
import org.neo4j.gds.core.utils.paged.HugeDoubleArray;
import org.neo4j.gds.core.utils.paged.HugeIntArray;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.core.utils.paged.HugeObjectArray;

public final class NodePropertyValuesAdapter {
    private NodePropertyValuesAdapter() {}

    public static LongNodePropertyValues adapt(HugeLongArray hugeLongArray) {
        return LongNodePropertyValuesAdapter.adapt(hugeLongArray);
    }

    public static LongNodePropertyValues adapt(HugeIntArray hugeIntArray) {
        return LongNodePropertyValuesAdapter.adapt(hugeIntArray);
    }

    public static LongNodePropertyValues adapt(HugeByteArray hugeByteArray) {
        return LongNodePropertyValuesAdapter.adapt(hugeByteArray);
    }

    public static DoubleNodePropertyValues adapt(HugeDoubleArray hugeDoubleArray) {
        return DoubleNodePropertyValuesAdapter.adapt(hugeDoubleArray);
    }

    public static NodePropertyValues adapt(HugeObjectArray<?> hugeObjectArray) {
        return ObjectNodePropertyValuesAdapter.adapt(hugeObjectArray);
    }

    public static LongNodePropertyValues adapt(HugeAtomicLongArray hugeAtomicLongArray) {
        return LongNodePropertyValuesAdapter.adapt(hugeAtomicLongArray);
    }

    public static DoubleNodePropertyValues adapt(HugeAtomicDoubleArray hugeAtomicDoubleArray) {
        return DoubleNodePropertyValuesAdapter.adapt(hugeAtomicDoubleArray);
    }
}

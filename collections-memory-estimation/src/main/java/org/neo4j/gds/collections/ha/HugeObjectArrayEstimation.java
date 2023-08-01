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
package org.neo4j.gds.collections.ha;

import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.mem.MemoryEstimations;
import org.neo4j.gds.core.utils.mem.MemoryRange;
import org.neo4j.gds.mem.HugeArrays;

import static org.neo4j.gds.mem.HugeArrays.PAGE_SIZE;
import static org.neo4j.gds.mem.HugeArrays.numberOfPages;
import static org.neo4j.gds.mem.MemoryUsage.sizeOfInstance;
import static org.neo4j.gds.mem.MemoryUsage.sizeOfObjectArray;

public final class HugeObjectArrayEstimation {

    private HugeObjectArrayEstimation() {}

    // TODO: lets remove this method
    public static MemoryEstimation objectArray(long objectEstimation) {
        return objectArray(
            MemoryEstimations.of("instance", (dimensions, concurrency) -> MemoryRange.of(objectEstimation))
        );
    }

    // TODO: let's remove this method
    public static MemoryEstimation objectArray(MemoryEstimation objectEstimation) {
        var builder = MemoryEstimations.builder();

        builder.perNode("instance", nodeCount -> {
            if (nodeCount <= HugeArrays.MAX_ARRAY_LENGTH) {
                return sizeOfInstance(HugeObjectArray.SingleHugeObjectArray.class);
            } else {
                return sizeOfInstance(HugeObjectArray.PagedHugeObjectArray.class);
            }
        });

        builder.perNode("data", objectEstimation);

        builder.perNode("pages", nodeCount -> {
            if (nodeCount <= HugeArrays.MAX_ARRAY_LENGTH) {
                return sizeOfObjectArray(nodeCount);
            } else {
                int numPages = numberOfPages(nodeCount);
                return sizeOfObjectArray(numPages) + numPages * sizeOfObjectArray(PAGE_SIZE);
            }
        });
        return builder.build();
    }


}

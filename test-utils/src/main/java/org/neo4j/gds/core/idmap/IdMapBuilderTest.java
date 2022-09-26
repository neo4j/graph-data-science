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
package org.neo4j.gds.core.idmap;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.core.loading.IdMapBuilder;
import org.neo4j.gds.core.loading.LabelInformation;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

public abstract class IdMapBuilderTest {

    protected abstract IdMapBuilder builder(long highestOriginalId, int concurrency);

    @Test
    void testAllocatedSize() {
        long capacity = 4096;
        int allocation = 1337;

        var builder = builder(capacity, 1);
        var idMapAllocator = builder.allocate(allocation);

        assertThat(idMapAllocator.allocatedSize()).as("Allocated size").isEqualTo(allocation);
    }

    @Test
    void testInsert() {
        long[] originalIds = new long[]{42, 43, 44, 1337};
        // number of ids we want to insert at once
        int batchLength = originalIds.length;
        // the highest original id defines the size of the specific IdMap data structures
        long highestOriginalId = Arrays.stream(originalIds).max().getAsLong();

        var builder = builder(highestOriginalId, 1);
        var idMapAllocator = builder.allocate(batchLength);

        idMapAllocator.insert(originalIds);

        var idMap = builder.build(LabelInformation.single(NodeLabel.of("A")), highestOriginalId, 1);

        assertThat(idMap.nodeCount()).as("node count").isEqualTo(batchLength);
        Arrays.stream(originalIds).forEach(originalId -> assertThat(idMap.contains(originalId))
            .as(originalId + " is contained in IdMap")
            .isTrue());
    }

}

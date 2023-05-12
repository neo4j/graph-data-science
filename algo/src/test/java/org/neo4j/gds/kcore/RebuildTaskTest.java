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
package org.neo4j.gds.kcore;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.core.concurrency.RunWithConcurrency;
import org.neo4j.gds.core.utils.paged.HugeIntArray;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.core.utils.partition.Partition;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class RebuildTaskTest {

    static Stream<Arguments> partitions() {
        return Stream.of(

            arguments(List.of(Partition.of(0,10))),
            arguments(List.of(Partition.of(0,4),Partition.of(4,6))));

    };
        @ParameterizedTest
        @MethodSource("partitions")
        void shouldRebuildCorrectly(List<Partition> partitions){
            var unassigned=KCoreDecomposition.UNASSIGNED;
            AtomicLong atomicLong=new AtomicLong();
            var core= HugeIntArray.of(unassigned,1,unassigned,1,unassigned,unassigned,1,unassigned,1,unassigned);
            HugeLongArray nodeOrder=HugeLongArray.newArray(6);
            List<RebuildTask> tasks=new ArrayList<>();
            for (Partition partition : partitions){
                tasks.add(new RebuildTask(partition,atomicLong,core,nodeOrder));
            }

            RunWithConcurrency.builder().concurrency(tasks.size()).tasks(tasks).run();

            assertThat(nodeOrder.toArray()).containsExactlyInAnyOrder(0,2,4,5,7,9);

            }
    }

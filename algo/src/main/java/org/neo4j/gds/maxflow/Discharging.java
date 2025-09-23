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
package org.neo4j.gds.maxflow;

import org.neo4j.gds.collections.ha.HugeDoubleArray;
import org.neo4j.gds.collections.ha.HugeLongArray;
import org.neo4j.gds.collections.haa.HugeAtomicDoubleArray;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.concurrency.ParallelUtil;
import org.neo4j.gds.core.concurrency.RunWithConcurrency;
import org.neo4j.gds.core.utils.paged.HugeAtomicBitSet;

import java.util.concurrent.atomic.AtomicLong;

public class Discharging {
    public static void processWorkingSet(
        FlowGraph flowGraph,
        HugeDoubleArray excess,
        HugeLongArray label,
        HugeLongArray tempLabel,
        HugeAtomicDoubleArray addedExcess,
        HugeAtomicBitSet isDiscovered,
        AtomicWorkingSet workingSet,
        long targetNode,
        long beta,
        AtomicLong workSinceLastGR,
        Concurrency concurrency
    ) {
        var dischargeTasks = ParallelUtil.tasks(
            concurrency,
            () -> new DischargeTask(
                flowGraph.concurrentCopy(),
                excess,
                label,
                tempLabel,
                addedExcess,
                isDiscovered,
                workingSet,
                targetNode,
                beta,
                workSinceLastGR
            )
        );

        //Discharge working set
        RunWithConcurrency.builder().concurrency(concurrency).tasks(dischargeTasks).build().run();
        workingSet.resetIdx();

        //Sync working set
        RunWithConcurrency.builder().concurrency(concurrency).tasks(dischargeTasks).build().run();
        workingSet.reset();

        //Update and sync new working set
        RunWithConcurrency.builder().concurrency(concurrency).tasks(dischargeTasks).build().run();
    }
}

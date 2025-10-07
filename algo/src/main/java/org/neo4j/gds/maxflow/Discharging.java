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
import org.neo4j.gds.core.concurrency.RunWithConcurrency;
import org.neo4j.gds.core.utils.paged.HugeAtomicBitSet;
import org.neo4j.gds.core.utils.paged.HugeLongArrayQueue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

final class Discharging {
    private final AtomicWorkingSet workingSet;
    private final Concurrency concurrency;
    private final Collection<Runnable> dischargeTasks;


    static Discharging createDischarging(
        FlowGraph flowGraph,
        HugeDoubleArray excess,
        HugeLongArray label,
        HugeAtomicDoubleArray addedExcess,
        AtomicWorkingSet workingSet,
        long targetNode,
        long beta,
        AtomicLong workSinceLastGR,
        Concurrency concurrency,
        HugeLongArrayQueue[] threadQueues
    )
    {

        var tempLabel = HugeLongArray.newArray(flowGraph.nodeCount());
        var isDiscovered = HugeAtomicBitSet.create(flowGraph.nodeCount());

        List<Runnable> dischargeTasks = new ArrayList<>();
        for (int i = 0; i < concurrency.value(); i++) {
            dischargeTasks.add(new DischargeTask(
                flowGraph.concurrentCopy(),
                excess,
                label,
                tempLabel,
                addedExcess,
                isDiscovered,
                workingSet,
                targetNode,
                beta,
                workSinceLastGR,
                threadQueues[i]
            ));
        }

        return new Discharging(workingSet, dischargeTasks, concurrency);
    }

    private Discharging(AtomicWorkingSet workingSet, Collection<Runnable> dischargeTasks, Concurrency concurrency) {
        this.workingSet = workingSet;
        this.dischargeTasks = dischargeTasks;
        this.concurrency = concurrency;
    }

    void processWorkingSet() {
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

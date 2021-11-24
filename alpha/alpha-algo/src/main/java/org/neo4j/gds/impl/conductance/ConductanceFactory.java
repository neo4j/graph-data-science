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
package org.neo4j.gds.impl.conductance;

import org.neo4j.gds.AlgorithmFactory;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.core.utils.mem.AllocationTracker;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.gds.core.utils.progress.tasks.Tasks;

public class ConductanceFactory<CONFIG extends ConductanceConfig> extends AlgorithmFactory<Conductance, CONFIG> {

    public ConductanceFactory() {
        super();
    }

    @Override
    protected String taskName() {
        return "Conductance";
    }

    @Override
    protected Conductance build(
        Graph graph, CONFIG configuration, AllocationTracker allocationTracker, ProgressTracker progressTracker
    ) {
        return new Conductance(graph, Pools.DEFAULT, configuration, progressTracker, allocationTracker);
    }

    @Override
    public Task progressTask(Graph graph, CONFIG config) {
        return Tasks.task(
            taskName(),
            Tasks.leaf("count relationships", graph.nodeCount()),
            Tasks.leaf("accumulate counts"),
            Tasks.leaf("perform conductance computations")
        );
    }
}

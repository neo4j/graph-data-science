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
package org.neo4j.gds.wcc;

import org.neo4j.gds.GraphAlgorithmFactory;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.concurrency.DefaultPool;
import org.neo4j.gds.core.concurrency.ParallelUtil;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.mem.MemoryEstimations;
import org.neo4j.gds.core.utils.paged.dss.HugeAtomicDisjointSetStruct;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.gds.core.utils.progress.tasks.Tasks;

public final class WccAlgorithmFactory<CONFIG extends WccBaseConfig> extends GraphAlgorithmFactory<Wcc, CONFIG> {

    public WccAlgorithmFactory() {
        super();
    }

    @Override
    public String taskName() {
        return "WCC";
    }

    public Wcc build(
        Graph graph,
        WccParameters parameters,
        ProgressTracker progressTracker
    ) {
        return new Wcc(
            graph,
            DefaultPool.INSTANCE,
            ParallelUtil.DEFAULT_BATCH_SIZE,
            parameters,
            progressTracker
        );
    }

    @Override
    public Wcc build(
        Graph graph,
        CONFIG configuration,
        ProgressTracker progressTracker
    ) {
        if (configuration.hasRelationshipWeightProperty() && configuration.threshold() == 0) {
            progressTracker.logWarning("Specifying a `relationshipWeightProperty` has no effect unless `threshold` is also set.");
        }
        return build(graph, configuration.toParameters(), progressTracker);
    }

    public Task progressTask(Graph graph) {
        return Tasks.leaf(taskName(), graph.relationshipCount());
    }

    @Override
    public Task progressTask(Graph graph, CONFIG config) {
        return progressTask(graph);
    }

    public MemoryEstimation memoryEstimation(boolean isIncremental) {
        return MemoryEstimations
            .builder(Wcc.class.getSimpleName())
            .add("dss", HugeAtomicDisjointSetStruct.memoryEstimation(isIncremental))
            .build();
    }

    @Override
    public MemoryEstimation memoryEstimation(CONFIG config) {
        return new WccMemoryEstimateDefinition(config.isIncremental())
            .memoryEstimation();
    }
}

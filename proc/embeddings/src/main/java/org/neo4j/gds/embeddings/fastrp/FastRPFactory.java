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
package org.neo4j.gds.embeddings.fastrp;

import org.neo4j.gds.AbstractAlgorithmFactory;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.utils.mem.AllocationTracker;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.progress.v2.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.v2.tasks.Task;
import org.neo4j.gds.core.utils.progress.v2.tasks.Tasks;
import org.neo4j.gds.ml.core.features.FeatureExtraction;

import java.util.List;

public class FastRPFactory<CONFIG extends FastRPBaseConfig> extends AbstractAlgorithmFactory<FastRP, CONFIG> {

    @Override
    protected String taskName() {
        return "FastRP";
    }

    @Override
    protected FastRP build(
        Graph graph, CONFIG configuration, AllocationTracker tracker, ProgressTracker progressTracker
    ) {
        var featureExtractors = FeatureExtraction.propertyExtractors(graph, configuration.featureProperties());

        return new FastRP(
            graph,
            configuration,
            featureExtractors,
            progressTracker,
            tracker
        );
    }

    @Override
    public MemoryEstimation memoryEstimation(CONFIG configuration) {
        return FastRP.memoryEstimation(configuration);
    }

    @Override
    public Task progressTask(Graph graph, CONFIG config) {
        return Tasks.task(
            taskName(),
            Tasks.leaf("Initialize Random Vectors", graph.nodeCount()),
            Tasks.iterativeFixed(
                "Propagate embeddings",
                () -> List.of(Tasks.leaf("Propagate embeddings task", graph.relationshipCount())),
                config.iterationWeights().size()
            )
        );
    }
}

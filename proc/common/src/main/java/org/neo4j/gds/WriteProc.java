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
package org.neo4j.gds;

import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.NodeProperties;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.config.WritePropertyConfig;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.core.utils.ProgressTimer;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.TaskProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.Tasks;
import org.neo4j.gds.core.write.ImmutableNodeProperty;
import org.neo4j.gds.core.write.NodeProperty;
import org.neo4j.gds.core.write.NodePropertyExporter;
import org.neo4j.gds.result.AbstractResultBuilder;

import java.util.List;
import java.util.stream.Stream;

public abstract class WriteProc<
    ALGO extends Algorithm<ALGO, ALGO_RESULT>,
    ALGO_RESULT,
    PROC_RESULT,
    CONFIG extends WritePropertyConfig & AlgoBaseConfig> extends NodePropertiesWriter<ALGO, ALGO_RESULT, CONFIG> {

    protected abstract AbstractResultBuilder<PROC_RESULT> resultBuilder(ComputationResult<ALGO, ALGO_RESULT, CONFIG> computeResult);

    @Override
    protected NodeProperties nodeProperties(ComputationResult<ALGO, ALGO_RESULT, CONFIG> computationResult) {
        throw new UnsupportedOperationException("Write procedures must implement either `nodeProperties` or `nodePropertyList`.");
    }

    protected List<NodeProperty> nodePropertyList(ComputationResult<ALGO, ALGO_RESULT, CONFIG> computationResult) {
        return List.of(ImmutableNodeProperty.of(computationResult.config().writeProperty(), nodeProperties(computationResult)));
    }

    protected Stream<PROC_RESULT> write(ComputationResult<ALGO, ALGO_RESULT, CONFIG> computeResult) {
        return runWithExceptionLogging("Graph write failed", () -> {
            CONFIG config = computeResult.config();

            AbstractResultBuilder<PROC_RESULT> builder = resultBuilder(computeResult)
                .withCreateMillis(computeResult.createMillis())
                .withComputeMillis(computeResult.computeMillis())
                .withNodeCount(computeResult.graph().nodeCount())
                .withConfig(config);

            if (!computeResult.isGraphEmpty()) {
                writeToNeo(builder, computeResult);
                computeResult.graph().releaseProperties();
            }
            return Stream.of(builder.build());
        });
    }

    void writeToNeo(
        AbstractResultBuilder<?> resultBuilder,
        ComputationResult<ALGO, ALGO_RESULT, CONFIG> computationResult
    ) {
        try (ProgressTimer ignored = ProgressTimer.start(resultBuilder::withWriteMillis)) {
            Graph graph = computationResult.graph();
            var progressTracker = createProgressTracker(graph, computationResult);
            var exporter = createNodePropertyExporter(graph, progressTracker, computationResult);

            try {
                progressTracker.beginSubTask();
                exporter.write(nodePropertyList(computationResult));
            } finally {
                progressTracker.endSubTask();
                progressTracker.release();
            }

            resultBuilder.withNodeCount(computationResult.graph().nodeCount());
            resultBuilder.withNodePropertiesWritten(exporter.propertiesWritten());
        }
    }

    ProgressTracker createProgressTracker(
        Graph graph,
        ComputationResult<ALGO, ALGO_RESULT, CONFIG> computationResult
    ) {
        var task = Tasks.leaf(algoName() + " :: WriteNodeProperties", graph.nodeCount());
        return new TaskProgressTracker(task, log, computationResult.config().writeConcurrency(), taskRegistryFactory);
    }

    NodePropertyExporter createNodePropertyExporter(
        Graph graph,
        ProgressTracker progressTracker,
        ComputationResult<ALGO, ALGO_RESULT, CONFIG> computationResult
    ) {
        return nodePropertyExporterBuilder
            .withIdMapping(graph)
            .withTerminationFlag(computationResult.algorithm().terminationFlag)
            .withProgressTracker(progressTracker)
            .parallel(Pools.DEFAULT, computationResult.config().writeConcurrency())
            .build();
    }
}

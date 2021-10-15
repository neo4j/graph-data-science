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
import org.neo4j.gds.config.WriteRelationshipConfig;
import org.neo4j.gds.core.utils.ProgressTimer;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.TaskProgressTracker;
import org.neo4j.gds.core.write.RelationshipStreamExporter;
import org.neo4j.gds.core.write.RelationshipStreaming;
import org.neo4j.gds.result.AbstractResultBuilder;

import java.util.stream.Stream;

public abstract class WriteStreamOfRelationshipsProc<
    ALGO extends Algorithm<ALGO, ALGO_RESULT>,
    ALGO_RESULT extends RelationshipStreaming,
    PROC_RESULT,
    CONFIG extends WriteRelationshipConfig & WritePropertyConfig & AlgoBaseConfig> extends StreamOfRelationshipsWriter<ALGO, ALGO_RESULT, CONFIG> {

    protected abstract AbstractResultBuilder<PROC_RESULT> resultBuilder(ComputationResult<ALGO, ALGO_RESULT, CONFIG> computeResult);

    @Override
    protected NodeProperties nodeProperties(ComputationResult<ALGO, ALGO_RESULT, CONFIG> computationResult) {
        throw new UnsupportedOperationException("Write relationship procedures do not produce node properties.");
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

    private void writeToNeo(
        AbstractResultBuilder<?> resultBuilder,
        ComputationResult<ALGO, ALGO_RESULT, CONFIG> computationResult
    ) {
        var config = computationResult.config();
        try (ProgressTimer ignored = ProgressTimer.start(resultBuilder::withWriteMillis)) {
            log.debug("Writing results");

            Graph graph = computationResult.graph();

            var progressTracker = new TaskProgressTracker(
                RelationshipStreamExporter.baseTask(algoName()),
                log,
                computationResult.config().writeConcurrency(),
                taskRegistryFactory
            );

            var relationshipsWritten = 0L;
            try {
                relationshipsWritten = createRelationshipStreamExporter(
                    graph,
                    progressTracker,
                    computationResult
                ).write(config.writeRelationshipType(), config.writeProperty());
            } finally {
                progressTracker.release();
            }

            resultBuilder.withRelationshipsWritten(relationshipsWritten);
        }
    }

    private RelationshipStreamExporter createRelationshipStreamExporter(
        Graph graph,
        ProgressTracker progressTracker,
        ComputationResult<ALGO, ALGO_RESULT, CONFIG> computationResult
    ) {
        return relationshipStreamExporterBuilder
            .withIdMapping(graph)
            .withTerminationFlag(computationResult.algorithm().terminationFlag)
            .withRelationships(computationResult.result().relationshipStream())
            .withProgressTracker(progressTracker)
            .build();
    }
}

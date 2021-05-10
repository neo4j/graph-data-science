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
package org.neo4j.graphalgo;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.NodeProperties;
import org.neo4j.graphalgo.config.AlgoBaseConfig;
import org.neo4j.graphalgo.config.WritePropertyConfig;
import org.neo4j.graphalgo.config.WriteRelationshipConfig;
import org.neo4j.graphalgo.core.utils.ProgressTimer;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphalgo.core.write.NodePropertyExporter;
import org.neo4j.graphalgo.core.write.RelationshipStreamExporter;
import org.neo4j.graphalgo.core.write.RelationshipStreaming;
import org.neo4j.graphalgo.result.AbstractResultBuilder;

import java.util.List;
import java.util.stream.Stream;

public abstract class WriteRelationshipsProc<
    ALGO extends Algorithm<ALGO, ALGO_RESULT>,
    ALGO_RESULT extends RelationshipStreaming,
    PROC_RESULT,
    CONFIG extends WriteRelationshipConfig & WritePropertyConfig & AlgoBaseConfig> extends AlgoBaseProc<ALGO, ALGO_RESULT, CONFIG> {

    protected abstract AbstractResultBuilder<PROC_RESULT> resultBuilder(ComputationResult<ALGO, ALGO_RESULT, CONFIG> computeResult);

    @Override
    protected NodeProperties nodeProperties(ComputationResult<ALGO, ALGO_RESULT, CONFIG> computationResult) {
        throw new UnsupportedOperationException("Write relationship procedures do not produce node properties.");
    }

    protected List<NodePropertyExporter.NodeProperty> nodePropertyList(ComputationResult<ALGO, ALGO_RESULT, CONFIG> computationResult) {
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
            TerminationFlag terminationFlag = computationResult.algorithm().getTerminationFlag();
            RelationshipStreamExporter exporter = RelationshipStreamExporter
                .builder(api, graph.cloneIdMapping(), computationResult.result().relationshipStream(), terminationFlag)
                .build();

            long numberOfRelationshipsWritten = exporter.write(config.writeRelationshipType(), config.writeProperty());

            resultBuilder.withRelationshipsWritten(numberOfRelationshipsWritten);
        }
    }
}

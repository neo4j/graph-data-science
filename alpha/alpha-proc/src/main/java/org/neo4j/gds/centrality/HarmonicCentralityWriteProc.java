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
package org.neo4j.gds.centrality;

import org.neo4j.gds.api.properties.nodes.DoubleNodePropertyValues;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.core.utils.ProgressTimer;
import org.neo4j.gds.core.utils.progress.tasks.TaskProgressTracker;
import org.neo4j.gds.core.write.NodePropertyExporter;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.impl.closeness.HarmonicCentralityConfig;
import org.neo4j.gds.impl.harmonic.HarmonicCentrality;
import org.neo4j.gds.impl.harmonic.HarmonicResult;
import org.neo4j.gds.result.AbstractCentralityResultBuilder;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.stream.Stream;

import static org.neo4j.gds.centrality.HarmonicCentralityProc.DESCRIPTION;
import static org.neo4j.gds.executor.ExecutionMode.WRITE_NODE_PROPERTY;
import static org.neo4j.procedure.Mode.WRITE;

@GdsCallable(name = "gds.alpha.closeness.harmonic.write", description = DESCRIPTION, executionMode = WRITE_NODE_PROPERTY)
public class HarmonicCentralityWriteProc extends HarmonicCentralityProc<CentralityScore.Stats> {

    @Procedure(value = "gds.alpha.closeness.harmonic.write", mode = WRITE)
    @Description(DESCRIPTION)
    public Stream<CentralityScore.Stats> write(
        @Name(value = "graphName") String graphName,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        var computationResult = compute(graphName, configuration);
        return computationResultConsumer().consume(computationResult, executionContext());
    }

    @Override
    public ComputationResultConsumer<HarmonicCentrality, HarmonicResult, HarmonicCentralityConfig, Stream<CentralityScore.Stats>> computationResultConsumer() {
        return (computationResult, executionContext) -> {
            var result = computationResult.result();
            var algorithm = computationResult.algorithm();
            var config = computationResult.config();
            var graph = computationResult.graph();

            AbstractCentralityResultBuilder<CentralityScore.Stats> builder = new CentralityScore.Stats.Builder(
                executionContext.callContext(),
                config.concurrency()
            );

            builder
                .withNodeCount(graph.nodeCount())
                .withConfig(config)
                .withComputeMillis(computationResult.computeMillis())
                .withPreProcessingMillis(computationResult.preProcessingMillis());

            if (graph.isEmpty()) {
                return Stream.of(builder.build());
            }

            builder.withCentralityFunction(computationResult.result()::getCentralityScore);

            try (ProgressTimer ignore = ProgressTimer.start(builder::withWriteMillis)) {
                var writeConcurrency = computationResult.config().writeConcurrency();
                var progressTracker = new TaskProgressTracker(
                    NodePropertyExporter.baseTask("HarmonicCentrality", graph.nodeCount()),
                    log,
                    writeConcurrency,
                    executionContext.taskRegistryFactory()
                );
                NodePropertyExporter exporter =  nodePropertyExporterBuilder
                    .withIdMap(graph)
                    .withTerminationFlag(algorithm.getTerminationFlag())
                    .withProgressTracker(progressTracker)
                    .parallel(Pools.DEFAULT, writeConcurrency)
                    .build();

                var properties = new DoubleNodePropertyValues() {
                    @Override
                    public long size() {
                        return computationResult.graph().nodeCount();
                    }

                    @Override
                    public double doubleValue(long nodeId) {
                        return result.getCentralityScore(nodeId);
                    }
                };

                exporter.write(
                    config.writeProperty(),
                    properties
                );
            }

            return Stream.of(builder.build());
        };
    }
}

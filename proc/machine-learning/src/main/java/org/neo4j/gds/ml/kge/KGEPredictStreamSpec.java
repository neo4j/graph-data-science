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
package org.neo4j.gds.ml.kge;

import org.neo4j.gds.algorithms.machinelearning.KGEPredictResult;
import org.neo4j.gds.algorithms.machinelearning.KGEPredictStreamConfig;
import org.neo4j.gds.algorithms.machinelearning.TopKMapComputer;
import org.neo4j.gds.executor.AlgorithmSpec;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.procedures.algorithms.configuration.NewConfigFunction;
import org.neo4j.gds.procedures.algorithms.machinelearning.KGEStreamResult;

import java.util.stream.Stream;

import static org.neo4j.gds.LoggingUtil.runWithExceptionLogging;
import static org.neo4j.gds.executor.ExecutionMode.STREAM;

@GdsCallable(
    name = "gds.ml.kge.predict.stream",
    description = "Predicts new relationships using an existing KGE model",
    executionMode = STREAM
)
public class KGEPredictStreamSpec implements AlgorithmSpec<
    TopKMapComputer,
    KGEPredictResult,
    KGEPredictStreamConfig,
    Stream<KGEStreamResult>,
    KGEPredictAlgorithmFactory<KGEPredictStreamConfig>> {
    @Override
    public String name() {
        return "KGEPredictStream";
    }

    @Override
    public KGEPredictAlgorithmFactory<KGEPredictStreamConfig> algorithmFactory(ExecutionContext executionContext) {
        return new KGEPredictAlgorithmFactory<>();
    }

    @Override
    public NewConfigFunction<KGEPredictStreamConfig> newConfigFunction() {
        return (__, config) -> KGEPredictStreamConfig.of(config);
    }

    @Override
    public ComputationResultConsumer<TopKMapComputer, KGEPredictResult, KGEPredictStreamConfig, Stream<KGEStreamResult>> computationResultConsumer() {
        return (computationResult, executionContext) -> runWithExceptionLogging(
            "Result streaming failed",
            executionContext.log(),
            () -> computationResult.result()
                .map(result -> {
                    var graphStore = computationResult.graphStore();
                    var graph = graphStore.getGraph();

                    return result.topKMap().stream((node1, node2, similarity) -> new KGEStreamResult(
                        graph.toOriginalNodeId(node1),
                        graph.toOriginalNodeId(node2),
                        similarity
                    ));
                }).orElseGet(Stream::empty)
        );
    }

}

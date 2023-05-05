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
package org.neo4j.gds.ml.linkmodels.pipeline.predict;

import org.neo4j.gds.executor.AlgorithmSpec;
import org.neo4j.gds.executor.ComputationResult;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.executor.NewConfigFunction;
import org.neo4j.gds.ml.linkmodels.LinkPredictionResult;

import java.util.Collections;
import java.util.stream.Stream;

import static org.neo4j.gds.executor.ExecutionMode.MUTATE_RELATIONSHIP;
import static org.neo4j.gds.ml.linkmodels.pipeline.LinkPredictionPipelineCompanion.PREDICT_DESCRIPTION;

@GdsCallable(name = "gds.beta.pipeline.linkPrediction.predict.mutate", description = PREDICT_DESCRIPTION, executionMode = MUTATE_RELATIONSHIP)
public class LinkPredictionPipelineMutateSpec implements AlgorithmSpec<
    LinkPredictionPredictPipelineExecutor,
    LinkPredictionResult,
    LinkPredictionPredictPipelineMutateConfig,
    Stream<MutateResult>,
    LinkPredictionPredictPipelineAlgorithmFactory<LinkPredictionPredictPipelineMutateConfig>> {
    @Override
    public String name() {
        return "LinkPredictionPipelineMutate";
    }

    @Override
    public LinkPredictionPredictPipelineAlgorithmFactory<LinkPredictionPredictPipelineMutateConfig> algorithmFactory(
        ExecutionContext executionContext) {
        return new LinkPredictionPredictPipelineAlgorithmFactory<>(executionContext);
    }

    @Override
    public NewConfigFunction<LinkPredictionPredictPipelineMutateConfig> newConfigFunction() {
        return LinkPredictionPredictPipelineMutateConfig::of;
    }

    @Override
    public ComputationResultConsumer<LinkPredictionPredictPipelineExecutor, LinkPredictionResult, LinkPredictionPredictPipelineMutateConfig, Stream<MutateResult>> computationResultConsumer() {
        return new LinkPredictionPipelineMutateResultConsumer(this::resultBuilder);
    }

    private MutateResult.Builder resultBuilder(
        ComputationResult<LinkPredictionPredictPipelineExecutor, LinkPredictionResult, LinkPredictionPredictPipelineMutateConfig> computeResult,
        ExecutionContext executionContext
    ) {
        var builder = new MutateResult.Builder()
            .withSamplingStats(computeResult.result()
                .map(LinkPredictionResult::samplingStats)
                .orElseGet(Collections::emptyMap));

        if (executionContext.returnColumns().contains("probabilityDistribution")) {
            builder.withHistogram();
        }
        return builder;
    }
}

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

import org.HdrHistogram.ConcurrentDoubleHistogram;
import org.jetbrains.annotations.Nullable;
import org.neo4j.gds.GraphStoreAlgorithmFactory;
import org.neo4j.gds.MutateComputationResultConsumer;
import org.neo4j.gds.MutateProc;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.core.Aggregation;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.ProcedureConstants;
import org.neo4j.gds.core.concurrency.ParallelUtil;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.core.loading.construction.GraphFactory;
import org.neo4j.gds.core.model.ModelCatalog;
import org.neo4j.gds.executor.ComputationResult;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.ml.linkmodels.LinkPredictionResult;
import org.neo4j.gds.result.AbstractResultBuilder;
import org.neo4j.gds.result.HistogramUtils;
import org.neo4j.gds.results.MemoryEstimateResult;
import org.neo4j.gds.results.StandardMutateResult;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Stream;

import static org.neo4j.gds.executor.ExecutionMode.MUTATE_RELATIONSHIP;
import static org.neo4j.gds.ml.linkmodels.pipeline.LinkPredictionPipelineCompanion.ESTIMATE_PREDICT_DESCRIPTION;
import static org.neo4j.gds.ml.linkmodels.pipeline.LinkPredictionPipelineCompanion.PREDICT_DESCRIPTION;
import static org.neo4j.gds.ml.pipeline.PipelineCompanion.preparePipelineConfig;

@GdsCallable(name = "gds.beta.pipeline.linkPrediction.predict.mutate", description = PREDICT_DESCRIPTION, executionMode = MUTATE_RELATIONSHIP)
public class LinkPredictionPipelineMutateProc extends MutateProc<LinkPredictionPredictPipelineExecutor, LinkPredictionResult, LinkPredictionPipelineMutateProc.MutateResult, LinkPredictionPredictPipelineMutateConfig> {

    @Context
    public ModelCatalog internalModelCatalog;

    @Procedure(name = "gds.beta.pipeline.linkPrediction.predict.mutate", mode = Mode.READ)
    @Description(PREDICT_DESCRIPTION)
    public Stream<LinkPredictionPipelineMutateProc.MutateResult> mutate(
        @Name(value = "graphName") String graphName,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        preparePipelineConfig(graphName, configuration);
        return mutate(compute(graphName, configuration));
    }

    @Procedure(name = "gds.beta.pipeline.linkPrediction.predict.mutate.estimate", mode = Mode.READ)
    @Description(ESTIMATE_PREDICT_DESCRIPTION)
    public Stream<MemoryEstimateResult> estimate(
        @Name(value = "graphNameOrConfiguration") Object graphNameOrConfiguration,
        @Name(value = "algoConfiguration") Map<String, Object> algoConfiguration
    ) {
        preparePipelineConfig(graphNameOrConfiguration, algoConfiguration);
        return computeEstimate(graphNameOrConfiguration, algoConfiguration);
    }

    @Override
    protected AbstractResultBuilder<MutateResult> resultBuilder(
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

    @Override
    public MutateComputationResultConsumer<LinkPredictionPredictPipelineExecutor, LinkPredictionResult, LinkPredictionPredictPipelineMutateConfig, MutateResult> computationResultConsumer() {
        return new MutateComputationResultConsumer<>(this::resultBuilder) {
            @Override
            protected void updateGraphStore(
                AbstractResultBuilder<?> resultBuilder,
                ComputationResult<LinkPredictionPredictPipelineExecutor, LinkPredictionResult, LinkPredictionPredictPipelineMutateConfig> computationResult,
                ExecutionContext executionContext
            ) {
                var graphStore = computationResult.graphStore();
                Collection<NodeLabel> labelFilter = computationResult.algorithm().labelFilter().predictNodeLabels();
                var graph = graphStore.getGraph(labelFilter);

                var config = computationResult.config();
                var concurrency = config.concurrency();
                var mutateRelationshipType = RelationshipType.of(config.mutateRelationshipType());

                var relationshipsBuilder = GraphFactory.initRelationshipsBuilder()
                    .aggregation(Aggregation.SINGLE)
                    .nodes(graph)
                    .relationshipType(mutateRelationshipType)
                    .orientation(Orientation.UNDIRECTED)
                    .addPropertyConfig(GraphFactory.PropertyConfig.of(computationResult.config().mutateProperty()))
                    .concurrency(concurrency)
                    .executorService(Pools.DEFAULT)
                    .build();

                var resultWithHistogramBuilder = (MutateResult.Builder) resultBuilder;
                var predictedLinkStream = computationResult.result()
                    .map(LinkPredictionResult::stream)
                    .orElseGet(Stream::empty);
                ParallelUtil.parallelStreamConsume(
                    predictedLinkStream,
                    concurrency,
                    stream -> stream.forEach(predictedLink -> {
                        relationshipsBuilder.addFromInternal(
                            graph.toRootNodeId(predictedLink.sourceId()),
                            graph.toRootNodeId(predictedLink.targetId()),
                            predictedLink.probability()
                        );
                        resultWithHistogramBuilder.recordHistogramValue(predictedLink.probability());
                    })
                );

                var relationships = relationshipsBuilder.build();



                computationResult
                    .graphStore()
                    .addRelationshipType(relationships);
                resultBuilder.withRelationshipsWritten(relationships.topology().elementCount());
            }
        };
    }

    @Override
    protected LinkPredictionPredictPipelineMutateConfig newConfig(String username, CypherMapWrapper config) {
        return LinkPredictionPredictPipelineMutateConfig.of(username, config);
    }

    @Override
    public GraphStoreAlgorithmFactory<LinkPredictionPredictPipelineExecutor, LinkPredictionPredictPipelineMutateConfig> algorithmFactory(
        ExecutionContext executionContext
    ) {
        return new LinkPredictionPredictPipelineAlgorithmFactory<>(executionContext);
    }

    @Override
    public ExecutionContext executionContext() {
        return super.executionContext().withModelCatalog(internalModelCatalog);
    }

    @SuppressWarnings("unused")
    public static final class MutateResult extends StandardMutateResult {

        public final long relationshipsWritten;
        public final Map<String, Object> probabilityDistribution;
        public final Map<String, Object> samplingStats;

        MutateResult(
            long preProcessingMillis,
            long computeMillis,
            long mutateMillis,
            long relationshipsWritten,
            Map<String, Object> configuration,
            Map<String, Object> probabilityDistribution,
            Map<String, Object> samplingStats
        ) {
            super(
                preProcessingMillis,
                computeMillis,
                0L,
                mutateMillis,
                configuration
            );

            this.relationshipsWritten = relationshipsWritten;
            this.probabilityDistribution = probabilityDistribution;
            this.samplingStats = samplingStats;
        }

        static class Builder extends AbstractResultBuilder<MutateResult> {

            private Map<String, Object> samplingStats = null;

            @Nullable
            private ConcurrentDoubleHistogram histogram = null;

            @Override
            public MutateResult build() {
                return new MutateResult(
                    preProcessingMillis,
                    computeMillis,
                    mutateMillis,
                    relationshipsWritten,
                    config.toMap(),
                    histogram == null ? Map.of() : HistogramUtils.similaritySummary(histogram),
                    samplingStats
                );
            }

            Builder withHistogram() {
                if (histogram != null) {
                    return this;
                }

                this.histogram = new ConcurrentDoubleHistogram(ProcedureConstants.HISTOGRAM_PRECISION_DEFAULT);
                return this;
            }

            void recordHistogramValue(double value) {
                if (histogram == null) {
                    return;
                }

                histogram.recordValue(value);
            }

            Builder withSamplingStats(Map<String, Object> samplingStats) {
                this.samplingStats = samplingStats;
                return this;
            }
        }
    }
}

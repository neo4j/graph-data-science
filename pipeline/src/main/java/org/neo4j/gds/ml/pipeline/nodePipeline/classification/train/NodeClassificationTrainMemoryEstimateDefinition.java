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
package org.neo4j.gds.ml.pipeline.nodePipeline.classification.train;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.neo4j.gds.collections.ha.HugeIntArray;
import org.neo4j.gds.collections.ha.HugeLongArray;
import org.neo4j.gds.collections.ha.HugeObjectArray;
import org.neo4j.gds.core.model.ModelCatalog;
import org.neo4j.gds.mem.MemoryEstimation;
import org.neo4j.gds.mem.MemoryEstimations;
import org.neo4j.gds.mem.MemoryRange;
import org.neo4j.gds.ml.api.TrainingMethod;
import org.neo4j.gds.ml.metrics.classification.ClassificationMetricSpecification;
import org.neo4j.gds.ml.models.ClassifierTrainerFactory;
import org.neo4j.gds.ml.models.automl.TunableTrainerConfig;
import org.neo4j.gds.ml.models.logisticregression.LogisticRegressionTrainConfig;
import org.neo4j.gds.ml.nodeClassification.ClassificationMetricComputer;
import org.neo4j.gds.ml.pipeline.NodePropertyStepExecutor;
import org.neo4j.gds.ml.pipeline.nodePipeline.NodePropertyPredictionSplitConfig;
import org.neo4j.gds.ml.pipeline.nodePipeline.classification.NodeClassificationTrainingPipeline;
import org.neo4j.gds.ml.splitting.FractionSplitter;
import org.neo4j.gds.ml.splitting.StratifiedKFoldSplitter;
import org.neo4j.gds.ml.training.TrainingStatistics;
import org.neo4j.gds.procedures.algorithms.AlgorithmsProcedureFacade;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.LongUnaryOperator;
import java.util.stream.Collectors;

import static org.neo4j.gds.mem.MemoryEstimations.delegateEstimation;
import static org.neo4j.gds.mem.MemoryEstimations.maxEstimation;
import static org.neo4j.gds.mem.Estimate.sizeOfDoubleArray;

public class NodeClassificationTrainMemoryEstimateDefinition {

    private final NodeClassificationTrainingPipeline pipeline;
    private final NodeClassificationPipelineTrainConfig configuration;
    private final ModelCatalog modelCatalog;
    private final AlgorithmsProcedureFacade algorithmsProcedureFacade;

    public NodeClassificationTrainMemoryEstimateDefinition(
        NodeClassificationTrainingPipeline pipeline,
        NodeClassificationPipelineTrainConfig configuration,
        @Nullable ModelCatalog modelCatalog,
        AlgorithmsProcedureFacade algorithmsProcedureFacade
    ) {
        this.pipeline = pipeline;
        this.configuration = configuration;
        this.modelCatalog = modelCatalog;
        this.algorithmsProcedureFacade = algorithmsProcedureFacade;
    }


    public MemoryEstimation memoryEstimation() {
        pipeline.validateTrainingParameterSpace();

        MemoryEstimation nodePropertyStepsEstimation = NodePropertyStepExecutor.estimateNodePropertySteps(
            algorithmsProcedureFacade,
            modelCatalog,
            configuration.username(),
            pipeline.nodePropertySteps(),
            configuration.nodeLabels(),
            configuration.relationshipTypes()
        );

        var trainingEstimation = MemoryEstimations
            .builder()
            .add("Training", estimateExcludingNodePropertySteps(
                configuration.metrics().size(),
                pipeline.splitConfig(),
                pipeline.trainingParameterSpace(), pipeline.numberOfModelSelectionTrials()
            ))
            .build();

        return MemoryEstimations.maxEstimation(
            "Node Classification Train Pipeline",
            List.of(nodePropertyStepsEstimation, trainingEstimation)
        );
    }

    private static MemoryEstimation estimateExcludingNodePropertySteps(
        int metricsSize,
        NodePropertyPredictionSplitConfig splitConfig,
        Map<TrainingMethod, List<TunableTrainerConfig>> trainingMethodListMap,
        int numberOfModelCandidates
    ) {
        var fudgedClassCount = 1000;
        var fudgedFeatureCount = 500;
        var testFraction = splitConfig.testFraction();

        var trainingParameterSpaces = trainingMethodListMap.values();

        var modelSelection = modelTrainAndEvaluateMemoryUsage(
            trainingParameterSpaces,
            fudgedClassCount,
            fudgedFeatureCount,
            splitConfig::foldTrainSetSize,
            splitConfig::foldTestSetSize
        );
        var bestModelEvaluation = delegateEstimation(
            modelTrainAndEvaluateMemoryUsage(
                trainingParameterSpaces,
                fudgedClassCount,
                fudgedFeatureCount,
                splitConfig::trainSetSize,
                splitConfig::testSetSize
            ),
            "best model evaluation"
        );

        var modelTrainingEstimation = maxEstimation(List.of(modelSelection, bestModelEvaluation));

        // Final step is to retrain the best model with the entire node set.
        // Training memory is independent of node set size so we can skip that last estimation.
        var builder = MemoryEstimations.builder()
            .perNode("global targets", HugeIntArray::memoryEstimation)
            .rangePerNode(
                "global class counts",
                __ -> MemoryRange.of(2L * Long.BYTES, (long) fudgedClassCount * Long.BYTES)
            )
            .add("metrics", ClassificationMetricSpecification.memoryEstimation(fudgedClassCount))
            .perNode("node IDs", HugeLongArray::memoryEstimation)
            .add("outer split", FractionSplitter.estimate(1 - testFraction))
            .add(
                "inner split",
                StratifiedKFoldSplitter.memoryEstimationForNodeSet(splitConfig.validationFolds(), 1 - testFraction)
            )
            .add(
                "stats map train",
                TrainingStatistics.memoryEstimationStatsMap(metricsSize, numberOfModelCandidates)
            )
            .add(
                "stats map validation",
                TrainingStatistics.memoryEstimationStatsMap(metricsSize, numberOfModelCandidates)
            )
            .add("max of model selection and best model evaluation", modelTrainingEstimation);

        if (!trainingMethodListMap.get(TrainingMethod.RandomForestClassification).isEmpty()) {
            // Having a random forest model candidate forces using eager feature extraction.
            builder.perGraphDimension("cached feature vectors", (dim, threads) -> MemoryRange.of(
                HugeObjectArray.memoryEstimation(dim.nodeCount(), sizeOfDoubleArray(10)),
                HugeObjectArray.memoryEstimation(dim.nodeCount(), sizeOfDoubleArray(fudgedFeatureCount))
            ));
        }

        return builder.build();
    }

    @NotNull
    private static MemoryEstimation modelTrainAndEvaluateMemoryUsage(
        Collection<List<TunableTrainerConfig>> trainingParameterSpaces,
        int fudgedClassCount,
        int fudgedFeatureCount,
        LongUnaryOperator trainSetSize,
        LongUnaryOperator testSetSize
    ) {
        var foldEstimations = trainingParameterSpaces
            .stream()
            .flatMap(List::stream)
            .flatMap(TunableTrainerConfig::streamCornerCaseConfigs)
            .map(
                config ->
                    MemoryEstimations.setup("max of training and evaluation", dim ->
                        {
                            var training = ClassifierTrainerFactory.memoryEstimation(
                                config,
                                trainSetSize,
                                (int) Math.min(fudgedClassCount, dim.nodeCount()),
                                MemoryRange.of(fudgedFeatureCount),
                                false
                            );

                            int batchSize = config instanceof LogisticRegressionTrainConfig
                                ? ((LogisticRegressionTrainConfig) config).batchSize()
                                : 0; // Not used
                            var evaluation = ClassificationMetricComputer.estimateEvaluation(
                                config,
                                (int) Math.min(batchSize, dim.nodeCount()),
                                trainSetSize,
                                testSetSize,
                                (int) Math.min(fudgedClassCount, dim.nodeCount()),
                                fudgedFeatureCount,
                                false
                            );

                            return MemoryEstimations.maxEstimation(List.of(training, evaluation));
                        }
                    ))
            .collect(Collectors.toList());

        return MemoryEstimations.builder("model selection")
            .max(foldEstimations)
            .build();
    }

}

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
package org.neo4j.gds.ml.linkmodels;

import org.neo4j.gds.ml.Training;
import org.neo4j.gds.ml.linkmodels.logisticregression.LinkFeatureCombiners;
import org.neo4j.gds.ml.linkmodels.logisticregression.LinkLogisticRegressionData;
import org.neo4j.gds.ml.linkmodels.logisticregression.LinkLogisticRegressionObjective;
import org.neo4j.gds.ml.linkmodels.logisticregression.LinkLogisticRegressionTrainConfig;
import org.neo4j.gds.ml.nodemodels.ImmutableMetricData;
import org.neo4j.gds.ml.nodemodels.ImmutableModelStats;
import org.neo4j.gds.ml.splitting.StratifiedKFoldSplitter;
import org.neo4j.graphalgo.RelationshipType;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimation;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimations;
import org.neo4j.graphalgo.core.utils.mem.MemoryRange;
import org.neo4j.graphalgo.core.utils.mem.MemoryUsage;
import org.neo4j.graphalgo.core.utils.paged.HugeLongArray;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

import static org.neo4j.graphalgo.core.utils.mem.MemoryEstimations.maxEstimation;
import static org.neo4j.graphalgo.core.utils.mem.MemoryUsage.sizeOfInstance;

public class LinkPredictionTrainEstimation {

    static MemoryEstimation estimate(LinkPredictionTrainConfig config) {
        var fudgedMinNodeFeatures = 500;
        var nodeFeatureDimension = Math.max(config.featureProperties().size(), fudgedMinNodeFeatures);
        // this is a max because we take the pessimistic stance and use the most expensive model
        // it stays in memory for the compute metric phases
        var modelDataEstimation = maxEstimation(
            config.paramConfigs()
                .stream()
                .map(llrConfig -> LinkLogisticRegressionData.memoryEstimation(getFeatureDimension(
                    llrConfig,
                    nodeFeatureDimension
                )))
                .collect(Collectors.toList())
        );
        return MemoryEstimations.builder()
            .perNode("node IDs", HugeLongArray::memoryEstimation)
            .max(List.of(
                estimateModelSelection(config, nodeFeatureDimension),
                estimateTrainModelOnEntireGraph(config, nodeFeatureDimension),
                estimateComputeTrainMetricPeak(config, modelDataEstimation),
                estimateComputeTestMetricPeak(config, modelDataEstimation)
            ))
            .add("model select result", estimateModelSelectResult(config))
            .add("metric results", estimateMetricsResult())
            .build();
    }

    private static MemoryEstimation estimateModelSelection(LinkPredictionTrainConfig config, int nodeFeatureDimension) {
        var maxOverParams = maxEstimation(config.paramConfigs()
            .stream()
            .map(llrConfig -> {
                var folds = (double) config.validationFolds();
                return MemoryEstimations.builder()
                    .fixed("stats map builder train", LinkPredictionTrain.ModelStatsBuilder.sizeInBytes())
                    .fixed("stats map builder validation", LinkPredictionTrain.ModelStatsBuilder.sizeInBytes())
                    .max(List.of(
                        estimateTrainModel(llrConfig, nodeFeatureDimension),
                        estimateComputeMetric(config.trainRelationshipType(), (folds - 1) / folds)
                    ))
                    .build();
            })
            .collect(Collectors.toList())
        );
        return MemoryEstimations.builder()
            .add("split", StratifiedKFoldSplitter.memoryEstimation(config.validationFolds(), 1.0))
            .add("stats map train", estimateStatsMap(config.params().size()))
            .add("stats map validation", estimateStatsMap(config.params().size()))
            .add(maxOverParams)
            .build();
    }

    private static MemoryEstimation estimateStatsMap(int numberOfParams) {
        var sizeOfOneModelStatsInBytes = sizeOfInstance(ImmutableModelStats.class);
        var sizeOfAllModelStatsInBytes = sizeOfOneModelStatsInBytes * numberOfParams;
        return MemoryEstimations.builder(HashMap.class)
            .fixed("array list", sizeOfInstance(ArrayList.class))
            .fixed("model stats", sizeOfAllModelStatsInBytes)
            .build();
    }

    private static MemoryEstimation estimateTrainModelOnEntireGraph(LinkPredictionTrainConfig config, int nodeFeatureDimension) {
        return MemoryEstimations.builder("train model on entire graph")
            .add(maxEstimation(
                config.paramConfigs()
                    .stream()
                    .map(llrConfig -> estimateTrainModel(llrConfig, nodeFeatureDimension))
                    .collect(Collectors.toList())
                )
            ).build();
    }

    private static MemoryEstimation estimateComputeTrainMetricPeak(LinkPredictionTrainConfig config, MemoryEstimation modelDataEstimation) {
        return MemoryEstimations.builder("compute train metrics")
            .add("model data", modelDataEstimation)
            .add(estimateComputeMetric(config.trainRelationshipType(), 1.0))
            .build();
    }

    private static MemoryEstimation estimateComputeTestMetricPeak(LinkPredictionTrainConfig config, MemoryEstimation modelDataEstimation) {
        return MemoryEstimations.builder("compute test metrics")
            .add("model data", modelDataEstimation)
            .add(estimateComputeMetric(config.testRelationshipType(), 1.0))
            .build();
    }

    private static MemoryEstimation estimateTrainModel(LinkLogisticRegressionTrainConfig llrConfig, int nodeFeatureDimension) {
        int featureDimension = getFeatureDimension(llrConfig, nodeFeatureDimension);
        return MemoryEstimations.builder()
            .add("model data", LinkLogisticRegressionData.memoryEstimation(featureDimension))
            .add("update weights", Training.memoryEstimation(featureDimension, 1, llrConfig.sharedUpdater(), 1))
            .perThread("computation graph", LinkLogisticRegressionObjective.sizeOfBatchInBytes(llrConfig.batchSize(), featureDimension))
            .build();
    }

    private static int getFeatureDimension(LinkLogisticRegressionTrainConfig llrConfig, int nodeFeatureDimension) {
        var linkFeatureCombiner = LinkFeatureCombiners.valueOf(llrConfig.linkFeatureCombiner());
        return linkFeatureCombiner.linkFeatureDimension(nodeFeatureDimension);
    }

    private static MemoryEstimation estimateComputeMetric(RelationshipType relationshipType, double relationshipFraction) {
        return MemoryEstimations.builder()
            .perGraphDimension(
                "signedProbabilities",
                ((dimensions, integer) -> MemoryRange.of(SignedProbabilities.estimateMemory(dimensions,
                    relationshipType,
                    relationshipFraction
                )))
            )
            .build();
    }

    private static MemoryEstimation estimateModelSelectResult(LinkPredictionTrainConfig config) {
        var numberOfParams = config.paramConfigs().size();
        var sizeOfOneModelStatsInBytes = sizeOfInstance(ImmutableModelStats.class);
        var sizeOfAllModelStatsInBytes = sizeOfOneModelStatsInBytes * numberOfParams;
        return MemoryEstimations.builder("model selection result")
            .fixed("instance", MemoryUsage.sizeOfInstance(ImmutableModelSelectResult.class))
            .fixed("model stats map train", MemoryUsage.sizeOfInstance(HashMap.class))
            .fixed("model stats list train", MemoryUsage.sizeOfInstance(ArrayList.class))
            .fixed("model stats map test", MemoryUsage.sizeOfInstance(HashMap.class))
            .fixed("model stats list test", MemoryUsage.sizeOfInstance(ArrayList.class))
            .fixed("model stats train", sizeOfAllModelStatsInBytes)
            .build();
    }

    private static MemoryEstimation estimateMetricsResult() {
        return MemoryEstimations.builder(HashMap.class)
            .fixed("metric data instance", sizeOfInstance(ImmutableMetricData.class))
            .fixed("train array list", sizeOfInstance(ArrayList.class))
            .fixed("validation array list", sizeOfInstance(ArrayList.class))
            .build();
    }
}

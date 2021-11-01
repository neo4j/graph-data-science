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
package org.neo4j.gds.ml.nodemodels;

import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.utils.TerminationFlag;
import org.neo4j.gds.core.utils.mem.AllocationTracker;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.ml.Predictor;
import org.neo4j.gds.ml.core.batch.BatchQueue;
import org.neo4j.gds.ml.core.tensor.Matrix;
import org.neo4j.gds.ml.nodemodels.logisticregression.NodeLogisticRegressionData;
import org.neo4j.gds.ml.nodemodels.metrics.ClassificationMetric;
import org.openjdk.jol.util.Multiset;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ClassificationMetricComputer implements MetricComputer {
    private final AllocationTracker allocationTracker;
    private final List<Metric> metrics;
    private final Multiset<Long> classCounts;
    private final Graph graph;
    private final NodeClassificationTrainConfig config;
    private final ProgressTracker progressTracker;
    private final TerminationFlag terminationFlag;

    public ClassificationMetricComputer(
        AllocationTracker allocationTracker,
        List<Metric> metrics,
        Multiset<Long> classCounts,
        Graph graph,
        NodeClassificationTrainConfig config,
        ProgressTracker progressTracker,
        TerminationFlag terminationFlag
    ) {
        this.allocationTracker = allocationTracker;
        this.metrics = metrics;
        this.classCounts = classCounts;
        this.graph = graph;
        this.config = config;
        this.progressTracker = progressTracker;
        this.terminationFlag = terminationFlag;
    }

    @Override
    public Map<Metric, Double> computeMetrics(
        HugeLongArray evaluationSet, Predictor<Matrix, ?> predictor
    ) {
        var predictedClasses = HugeLongArray.newArray(evaluationSet.size(), allocationTracker);

        // consume from queue which contains local nodeIds, i.e. indices into evaluationSet
        // the consumer internally remaps to original nodeIds before prediction
        var consumer = new NodeClassificationPredictConsumer(
            graph,
            evaluationSet::get,
            (Predictor<Matrix, NodeLogisticRegressionData>) predictor,
            null,
            predictedClasses,
            config.featureProperties(),
            progressTracker
        );

        var queue = new BatchQueue(evaluationSet.size());
        queue.parallelConsume(consumer, config.concurrency(), terminationFlag);

        var localTargets = makeLocalTargets(evaluationSet);
        return metrics.stream().collect(Collectors.toMap(
            metric -> metric,
            metric -> ((ClassificationMetric) metric).compute(localTargets, predictedClasses, classCounts)
        ));
    }

    private HugeLongArray makeLocalTargets(HugeLongArray nodeIds) {
        var targets = HugeLongArray.newArray(nodeIds.size(), allocationTracker);
        var targetNodeProperty = graph.nodeProperties(config.targetProperty());

        targets.setAll(i -> targetNodeProperty.longValue(nodeIds.get(i)));
        return targets;
    }

}

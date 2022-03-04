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

import org.neo4j.gds.core.utils.TerminationFlag;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.models.Features;
import org.neo4j.gds.models.Classifier;
import org.neo4j.gds.ml.core.batch.BatchQueue;
import org.neo4j.gds.ml.nodemodels.metrics.ClassificationMetric;
import org.openjdk.jol.util.Multiset;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ClassificationMetricComputer implements MetricComputer {
    private final List<Metric> metrics;
    private final Multiset<Long> classCounts;
    private final Features features;
    private final HugeLongArray targets;
    private final int concurrency;
    private final ProgressTracker progressTracker;
    private final TerminationFlag terminationFlag;

    public ClassificationMetricComputer(
        List<Metric> metrics,
        Multiset<Long> classCounts,
        Features features,
        HugeLongArray targets,
        int concurrency,
        ProgressTracker progressTracker,
        TerminationFlag terminationFlag
    ) {
        this.metrics = metrics;
        this.classCounts = classCounts;
        this.features = features;
        this.targets = targets;
        this.concurrency = concurrency;
        this.progressTracker = progressTracker;
        this.terminationFlag = terminationFlag;
    }

    @Override
    public Map<Metric, Double> computeMetrics(
        HugeLongArray evaluationSet, Classifier classifier
    ) {
        var predictedClasses = HugeLongArray.newArray(evaluationSet.size());

        // consume from queue which contains local nodeIds, i.e. indices into evaluationSet
        // the consumer internally remaps to original nodeIds before prediction
        var consumer = new NodeClassificationPredictConsumer(
            features,
            evaluationSet::get,
            classifier,
            null,
            predictedClasses,
            progressTracker
        );

        var queue = new BatchQueue(evaluationSet.size());
        queue.parallelConsume(consumer, concurrency, terminationFlag);

        var localLabels = makeLocalTargets(evaluationSet, targets);
        return metrics.stream().collect(Collectors.toMap(
            metric -> metric,
            metric -> ((ClassificationMetric) metric).compute(localLabels, predictedClasses, classCounts)
        ));
    }

    private HugeLongArray makeLocalTargets(HugeLongArray nodeIds, HugeLongArray targets) {
        var localTargets = HugeLongArray.newArray(nodeIds.size());

        localTargets.setAll(i -> targets.get(nodeIds.get(i)));
        return localTargets;
    }

}

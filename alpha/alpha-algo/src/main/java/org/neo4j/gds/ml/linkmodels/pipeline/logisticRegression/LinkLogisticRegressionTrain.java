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
package org.neo4j.gds.ml.linkmodels.pipeline.logisticRegression;

import org.neo4j.gds.core.utils.TerminationFlag;
import org.neo4j.gds.core.utils.paged.HugeDoubleArray;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.core.utils.paged.HugeObjectArray;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.ml.Training;
import org.neo4j.gds.ml.core.batch.BatchQueue;
import org.neo4j.gds.ml.core.batch.HugeBatchQueue;

import java.util.function.Supplier;

public class LinkLogisticRegressionTrain {

    private final HugeLongArray trainSet;
    private final HugeObjectArray<double[]> linkFeatures;
    private final HugeDoubleArray linkTargets;
    private final LinkLogisticRegressionTrainConfig config;
    private final ProgressTracker progressTracker;
    private final TerminationFlag terminationFlag;
    private final int concurrency;

    public LinkLogisticRegressionTrain(
        HugeLongArray trainSet,
        HugeObjectArray<double[]> linkFeatures,
        HugeDoubleArray linkTargets,
        LinkLogisticRegressionTrainConfig config,
        ProgressTracker progressTracker,
        TerminationFlag terminationFlag,
        int concurrency
    ) {
        this.trainSet = trainSet;
        this.linkFeatures = linkFeatures;
        this.linkTargets = linkTargets;
        this.config = config;
        this.progressTracker = progressTracker;
        this.terminationFlag = terminationFlag;
        this.concurrency = concurrency;
    }

    public LinkLogisticRegressionData compute() {
        assert linkFeatures.size() != 0;

        var llrData = LinkLogisticRegressionData.from(linkFeatures.get(0).length, config.useBiasFeature());
        var objective = new LinkLogisticRegressionObjective(llrData, config.penalty(), linkFeatures, linkTargets);
        var training = new Training(config, progressTracker, linkFeatures.size(), terminationFlag);
        Supplier<BatchQueue> queueSupplier = () -> new HugeBatchQueue(trainSet, config.batchSize());

        training.train(objective, queueSupplier, concurrency);

        return objective.modelData();
    }
}

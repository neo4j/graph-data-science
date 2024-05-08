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
package org.neo4j.gds.ml.nodePropertyPrediction.regression;

import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.concurrency.ParallelUtil;
import org.neo4j.gds.termination.TerminationFlag;
import org.neo4j.gds.collections.ha.HugeDoubleArray;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.gds.core.utils.progress.tasks.Tasks;
import org.neo4j.gds.ml.models.Features;
import org.neo4j.gds.ml.models.Regressor;

public class NodeRegressionPredict {

    private final Regressor regressor;
    private final Features features;
    private final Concurrency concurrency;
    private final ProgressTracker progressTracker;
    private final TerminationFlag terminationFlag;

    public NodeRegressionPredict(
        Regressor regressor,
        Features features,
        Concurrency concurrency,
        ProgressTracker progressTracker,
        TerminationFlag terminationFlag
    ) {
        this.regressor = regressor;
        this.features = features;
        this.concurrency = concurrency;
        this.progressTracker = progressTracker;
        this.terminationFlag = terminationFlag;
    }

    public static Task progressTask(long nodeCount) {
        return Tasks.leaf("Predict", nodeCount);
    }


    public HugeDoubleArray compute() {
        progressTracker.beginSubTask("Predict");
        var predictedTargets = HugeDoubleArray.newArray(features.size());
        ParallelUtil.parallelForEachNode(
            features.size(),
            concurrency,
            terminationFlag,
            id -> predictedTargets.set(id, regressor.predict(features.get(id)))
        );
        progressTracker.endSubTask("Predict");

        return predictedTargets;
    }
}

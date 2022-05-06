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

import org.neo4j.gds.Algorithm;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.concurrency.ParallelUtil;
import org.neo4j.gds.core.utils.TerminationFlag;
import org.neo4j.gds.core.utils.paged.HugeDoubleArray;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.gds.core.utils.progress.tasks.Tasks;
import org.neo4j.gds.ml.models.Features;
import org.neo4j.gds.ml.models.Regressor;

public class NodeRegressionPredict extends Algorithm<HugeDoubleArray> {

    private final Regressor regressor;
    private final Features features;
    private final int concurrency;

    public NodeRegressionPredict(
        Regressor regressor,
        Features features,
        int concurrency,
        ProgressTracker progressTracker,
        TerminationFlag terminationFlag
    ) {
        super(progressTracker);
        this.regressor = regressor;
        this.features = features;
        this.concurrency = concurrency;
        this.terminationFlag = terminationFlag;
    }

    public static Task progressTask(Graph graph) {
        return Tasks.leaf("Node Regression predict", graph.nodeCount());
    }


    @Override
    public HugeDoubleArray compute() {
        progressTracker.beginSubTask("Node Regression predict");
        var predictedTargets = HugeDoubleArray.newArray(features.size());
        ParallelUtil.parallelForEachNode(
            features.size(),
            concurrency,
            id -> predictedTargets.set(id, regressor.predict(features.get(id)))
        );
        progressTracker.endSubTask("Node Regression predict");

        return predictedTargets;
    }

    @Override
    public void release() {

    }
}

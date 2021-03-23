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
package org.neo4j.gds.ml.nodemodels.logisticregression;

import org.neo4j.gds.ml.Training;
import org.neo4j.gds.ml.batch.BatchQueue;
import org.neo4j.gds.ml.nodemodels.multiclasslogisticregression.MultiClassNLRTrainConfig;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.utils.ProgressLogger;

// TODO: remove?
public class NodeLogisticRegressionTrain {

    public static final String MODEL_TYPE = "nodeLogisticRegression";

    private final Graph graph;
    private final MultiClassNLRTrainConfig config;
    private final ProgressLogger progressLogger;

    public NodeLogisticRegressionTrain(
        Graph graph,
        MultiClassNLRTrainConfig config,
        ProgressLogger progressLogger
    ) {
        this.graph = graph;
        this.config = config;
        this.progressLogger = progressLogger;
    }

    public NodeLogisticRegressionData compute() {
        var objective = new NodeLogisticRegressionObjective(
            config.featureProperties(),
            config.targetProperty(),
            graph
        );

        var training = new Training(config, progressLogger, graph.nodeCount());
        training.train(objective, () -> new BatchQueue(graph.nodeCount(), config.batchSize()), 1);
        return objective.modelData();
    }
}

/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
import org.neo4j.gds.ml.TrainingSettings;
import org.neo4j.graphalgo.Algorithm;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.logging.Log;

public class NodeLogisticRegressionTrainAlgo extends Algorithm<NodeLogisticRegressionTrainAlgo, NodeLogisticRegression> {
    private final Graph graph;
    private final TrainingSettings trainingSettings;
    private final NodeLogisticRegressionTrainConfig config;
    private final Log log;

    public NodeLogisticRegressionTrainAlgo(
        Graph graph,
        TrainingSettings trainingSettings,
        NodeLogisticRegressionTrainConfig config,
        Log log
    ) {
        this.graph = graph;
        this.trainingSettings = trainingSettings;
        this.config = config;
        this.log = log;
    }

    @Override
    public NodeLogisticRegression compute() {
        NodeLogisticRegression model = new NodeLogisticRegression(
            config.featureProperties(),
            config.targetProperty(),
            graph
        );
        Training training = new Training(trainingSettings, log);
        training.train(model, () -> trainingSettings.batchQueue(graph.nodeCount()), config.concurrency());
        return model;
    }

    @Override
    public NodeLogisticRegressionTrainAlgo me() {
        return this;
    }

    @Override
    public void release() {

    }
}

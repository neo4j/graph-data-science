/*
 * Copyright (c) 2017-2021 "Neo4j,"
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
package org.neo4j.gds.ml.linkmodels.logisticregression;

import org.neo4j.gds.ml.Training;
import org.neo4j.gds.ml.TrainingSettings;
import org.neo4j.graphalgo.Algorithm;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.logging.Log;

public class LinkLogisticRegressionTrain extends Algorithm<LinkLogisticRegressionTrain, LinkLogisticRegressionPredictor> {
    private final Graph graph;
    private final TrainingSettings trainingSettings;
    private final LinkLogisticRegressionTrainConfig config;
    private final Log log;

    public LinkLogisticRegressionTrain(
        Graph graph,
        TrainingSettings trainingSettings,
        LinkLogisticRegressionTrainConfig config,
        Log log
    ) {
        this.graph = graph;
        this.trainingSettings = trainingSettings;
        this.config = config;
        this.log = log;
    }

    @Override
    public LinkLogisticRegressionPredictor compute() {
        var objective = new LinkLogisticRegressionObjective(
            config.featureProperties(),
            LinkFeatureCombiner.valueOf(config.linkFeatureCombiner()),
            graph
        );
        var training = new Training(trainingSettings, log, graph.nodeCount());
        training.train(objective, () -> trainingSettings.batchQueue(graph.nodeCount()), config.concurrency());
        return new LinkLogisticRegressionPredictor(objective.modelData);
    }

    @Override
    public LinkLogisticRegressionTrain me() {
        return this;
    }

    @Override
    public void release() {

    }
}

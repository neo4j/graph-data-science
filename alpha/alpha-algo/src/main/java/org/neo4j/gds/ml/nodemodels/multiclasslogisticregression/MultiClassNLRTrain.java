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
package org.neo4j.gds.ml.nodemodels.multiclasslogisticregression;

import org.neo4j.gds.ml.BatchQueue;
import org.neo4j.gds.ml.Training;
import org.neo4j.gds.ml.nodemodels.logisticregression.NodeLogisticRegressionTrainConfig;
import org.neo4j.graphalgo.Algorithm;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.model.Model;
import org.neo4j.logging.Log;

public class MultiClassNLRTrain
    extends Algorithm<MultiClassNLRTrain, Model<MultiClassNLRData, NodeLogisticRegressionTrainConfig>> {

    public static final String MODEL_TYPE = "multiClassNodeLogisticRegression";

    private final Graph graph;
    private final NodeLogisticRegressionTrainConfig config;
    private final Log log;

    public MultiClassNLRTrain(
        Graph graph,
        NodeLogisticRegressionTrainConfig config,
        Log log
    ) {
        this.graph = graph;
        this.config = config;
        this.log = log;
    }

    @Override
    public Model<MultiClassNLRData, NodeLogisticRegressionTrainConfig> compute() {
        var objective = new MultiClassNLRObjective(
            config.featureProperties(),
            config.targetProperty(),
            graph,
            config.penalty()
        );
        var training = new Training(config, log, graph.nodeCount());
        training.train(objective, () -> new BatchQueue(graph.nodeCount(), config.batchSize()), config.concurrency());

        return Model.of(
            config.username(),
            config.modelName(),
            MODEL_TYPE,
            graph.schema(),
            objective.modelData(),
            config
        );
    }

    @Override
    public MultiClassNLRTrain me() {
        return this;
    }

    @Override
    public void release() {

    }
}

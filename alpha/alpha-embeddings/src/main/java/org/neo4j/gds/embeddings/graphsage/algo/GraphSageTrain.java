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
package org.neo4j.gds.embeddings.graphsage.algo;

import org.neo4j.gds.embeddings.graphsage.GraphSageTrainModel;
import org.neo4j.graphalgo.Algorithm;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.NodeProperties;
import org.neo4j.graphalgo.core.model.Model;
import org.neo4j.logging.Log;

import java.util.List;

import static java.util.stream.Collectors.toList;
import static org.neo4j.gds.embeddings.graphsage.GraphSageHelper.initializeFeatures;
import static org.neo4j.gds.embeddings.graphsage.algo.GraphSage.ALGO_TYPE;

public class GraphSageTrain extends Algorithm<GraphSageTrain, Model<GraphSageModel>> {

    private final GraphSageTrainModel graphSageModel;
    private final Graph graph;
    private final GraphSageTrainConfig config;
    private final List<NodeProperties> nodeProperties;
    private final boolean useDegreeAsProperty;

    public GraphSageTrain(Graph graph, GraphSageTrainConfig config, Log log) {
        this.graph = graph;
        this.config = config;
        this.graphSageModel = new GraphSageTrainModel(config, log);
        this.useDegreeAsProperty = config.degreeAsProperty();
        this.nodeProperties = config
            .nodePropertyNames()
            .stream()
            .map(graph::nodeProperties)
            .collect(toList());
    }

    @Override
    public Model<GraphSageModel> compute() {
        GraphSageTrainModel.ModelTrainResult trainResult = graphSageModel.train(
            graph,
            initializeFeatures(graph, nodeProperties, useDegreeAsProperty)
        );
        return Model.of(config.modelName(), ALGO_TYPE, GraphSageModel.of(trainResult.layers(), nodeProperties, useDegreeAsProperty));
    }

    @Override
    public GraphSageTrain me() {
        return this;
    }

    @Override
    public void release() {

    }
}

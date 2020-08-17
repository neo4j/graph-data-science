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
import org.neo4j.gds.embeddings.graphsage.Layer;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.logging.Log;

public class GraphSageTrain extends GraphSageBaseAlgo<GraphSageTrain, GraphSageTrain.TrainedModel, GraphSageTrainConfig> {

    private final GraphSageTrainModel graphSageModel;
    private final GraphSageTrainConfig config;

    public GraphSageTrain(Graph graph, GraphSageTrainConfig config, Log log) {
        super(graph, config);

        this.config = config;
        this.graphSageModel = new GraphSageTrainModel(config, log);
    }

    @Override
    public TrainedModel compute() {
        GraphSageTrainModel.ModelTrainResult trainResult = graphSageModel.train(graph, initializeFeatures());
        return new TrainedModel(config.modelName(), ALGO_TYPE, trainResult.layers());
    }

    @Override
    public GraphSageTrain me() {
        return this;
    }

    @Override
    public void release() {

    }

    public static class TrainedModel {

        String modelName;
        String algoType;
        Layer[] layers;

        TrainedModel(String modelName, String algoType, Layer[] layers) {
            this.modelName = modelName;
            this.algoType = algoType;
            this.layers = layers;
        }

        public String modelName() {
            return modelName;
        }

        public String algoType() {
            return algoType;
        }

    }
}

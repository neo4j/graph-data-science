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
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.logging.Log;

public class GraphSageTrain extends GraphSageBase<GraphSageTrain, GraphSageTrain.TrainedModel, GraphSageTrainConfig> {

    private final GraphSageTrainModel graphSageModel;

    public GraphSageTrain(Graph graph, GraphSageTrainConfig config, Log log) {
        super(graph, config);

        graphSageModel = new GraphSageTrainModel(config, log);
    }

    @Override
    public TrainedModel compute() {
        graphSageModel.train(graph, initializeFeatures());
        return new TrainedModel();
    }

    @Override
    public GraphSageTrain me() {
        return this;
    }

    @Override
    public void release() {

    }

    public static class TrainedModel {

    }
}

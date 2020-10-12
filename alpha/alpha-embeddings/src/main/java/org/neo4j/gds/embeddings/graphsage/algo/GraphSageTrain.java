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

import org.neo4j.gds.embeddings.graphsage.GraphSageModelTrainer;
import org.neo4j.gds.embeddings.graphsage.ModelData;
import org.neo4j.graphalgo.Algorithm;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.model.Model;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;

import static org.neo4j.gds.embeddings.graphsage.GraphSageHelper.initializeFeatures;

public class GraphSageTrain extends Algorithm<GraphSageTrain, Model<ModelData, GraphSageTrainConfig>> {

    private final Graph graph;
    private final GraphSageTrainConfig config;
    private final AllocationTracker tracker;
    private final ProgressLogger progressLogger;

    public GraphSageTrain(
        Graph graph,
        GraphSageTrainConfig config,
        AllocationTracker tracker,
        ProgressLogger progressLogger
    ) {
        this.graph = graph;
        this.config = config;
        this.tracker = tracker;
        this.progressLogger = progressLogger;
    }

    @Override
    public Model<ModelData, GraphSageTrainConfig> compute() {
        var graphSageModel = new GraphSageModelTrainer(config, progressLogger);

        GraphSageModelTrainer.ModelTrainResult trainResult = graphSageModel.train(
            graph,
            initializeFeatures(graph, config, tracker)
        );

        return Model.of(
            config.username(),
            config.modelName(),
            GraphSage.MODEL_TYPE,
            graph.schema(),
            ModelData.of(trainResult.layers(), org.neo4j.gds.embeddings.graphsage.GraphSageHelper::features),
            config
        );
    }

    @Override
    public GraphSageTrain me() {
        return this;
    }

    @Override
    public void release() {

    }
}

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
package org.neo4j.gds.applications.algorithms.embeddings;

import org.neo4j.gds.NodeEmbeddingsAlgorithmTasks;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.applications.algorithms.machinery.ProgressTrackerCreator;
import org.neo4j.gds.core.model.Model;
import org.neo4j.gds.embeddings.fastrp.FastRPBaseConfig;
import org.neo4j.gds.embeddings.fastrp.FastRPConfigTransformer;
import org.neo4j.gds.embeddings.fastrp.FastRPResult;
import org.neo4j.gds.embeddings.graphsage.GraphSageModelTrainer;
import org.neo4j.gds.embeddings.graphsage.ModelData;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSageBaseConfig;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSageResult;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSageTrainConfig;
import org.neo4j.gds.embeddings.hashgnn.HashGNNConfig;
import org.neo4j.gds.embeddings.hashgnn.HashGNNConfigTransformer;
import org.neo4j.gds.embeddings.hashgnn.HashGNNResult;
import org.neo4j.gds.embeddings.node2vec.Node2VecBaseConfig;
import org.neo4j.gds.embeddings.node2vec.Node2VecConfigTransformer;
import org.neo4j.gds.embeddings.node2vec.Node2VecResult;

public class NodeEmbeddingBusinessAlgorithms {

    private final NodeEmbeddingAlgorithms algorithms;
    private final ProgressTrackerCreator progressTrackerCreator;
    private final NodeEmbeddingsAlgorithmTasks tasks = new NodeEmbeddingsAlgorithmTasks();

    public NodeEmbeddingBusinessAlgorithms(
        NodeEmbeddingAlgorithms algorithms,
        ProgressTrackerCreator progressTrackerCreator
    ) {
        this.algorithms = algorithms;
        this.progressTrackerCreator = progressTrackerCreator;
    }

    public FastRPResult fastRP(Graph graph, FastRPBaseConfig configuration) {
        var params = FastRPConfigTransformer.toParameters(configuration);
        var task = tasks.fastRP(graph,params);
        var progressTracker = progressTrackerCreator.createProgressTracker(task,configuration);

        return algorithms.fastRP(graph,params,progressTracker);

    }

    Node2VecResult node2Vec(Graph graph, Node2VecBaseConfig configuration) {
        var params = Node2VecConfigTransformer.node2VecParameters(configuration);
        var task = tasks.node2Vec(graph,params);
        var progressTracker = progressTrackerCreator.createProgressTracker(task,configuration);

        return algorithms.node2Vec(graph, params, progressTracker);
    }

    HashGNNResult hashGnn(Graph graph, HashGNNConfig configuration) {
        var params = HashGNNConfigTransformer.toParameters(configuration);
        var task = tasks.hashGNN(graph, params, configuration.relationshipTypes());
        var progressTracker = progressTrackerCreator.createProgressTracker(task,configuration);
        return algorithms.hashGnn(graph, params, progressTracker);
    }


    public GraphSageResult graphSage(Graph graph, GraphSageBaseConfig configuration) {
       return  algorithms.graphSage(graph,configuration);
    }

    public Model<ModelData, GraphSageTrainConfig, GraphSageModelTrainer.GraphSageTrainMetrics> graphSageTrain(
        Graph graph,
        GraphSageTrainConfig configuration
    ) {
       return algorithms.graphSageTrain(graph,configuration);
    }


}

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

import fastrp.FastRPParameters;
import hashgnn.HashGNNParameters;
import node2vec.Node2VecParameters;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmMachinery;
import org.neo4j.gds.core.concurrency.DefaultPool;
import org.neo4j.gds.core.model.Model;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.embeddings.fastrp.FastRP;
import org.neo4j.gds.embeddings.fastrp.FastRPResult;
import org.neo4j.gds.embeddings.graphsage.GraphSageModelTrainer;
import org.neo4j.gds.embeddings.graphsage.ModelData;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSage;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSageParameters;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSageResult;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSageTrainConfig;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSageTrainParameters;
import org.neo4j.gds.embeddings.hashgnn.HashGNN;
import org.neo4j.gds.embeddings.hashgnn.HashGNNResult;
import org.neo4j.gds.embeddings.node2vec.Node2Vec;
import org.neo4j.gds.embeddings.node2vec.Node2VecResult;
import org.neo4j.gds.ml.core.features.FeatureExtraction;
import org.neo4j.gds.termination.TerminationFlag;

public class NodeEmbeddingAlgorithms {

    private static final GraphSageTrainAlgorithmFactory graphSageTrainAlgorithmFactory = new GraphSageTrainAlgorithmFactory();
    private final AlgorithmMachinery algorithmMachinery = new AlgorithmMachinery();
    private final GraphSageModelCatalog graphSageModelCatalog;
    private final TerminationFlag terminationFlag;

    public NodeEmbeddingAlgorithms(
        GraphSageModelCatalog graphSageModelCatalog,
        TerminationFlag terminationFlag
    ) {
        this.graphSageModelCatalog = graphSageModelCatalog;
        this.terminationFlag = terminationFlag;
    }

    public FastRPResult fastRP(Graph graph, FastRPParameters parameters, ProgressTracker progressTracker) {

        var featureExtractors = FeatureExtraction.propertyExtractors(graph, parameters.featureProperties());

        var algorithm = new FastRP(
            graph,
            parameters,
            10_000,
            featureExtractors,
            progressTracker,
            terminationFlag
        );

        return algorithmMachinery.runAlgorithmsAndManageProgressTracker(
            algorithm,
            progressTracker,
            true,
            parameters.concurrency()
        );
    }


    public GraphSageResult graphSage(Graph graph, GraphSageParameters  parameters, ProgressTracker progressTracker) {
        var modeParameters  = parameters.modeParameters();
        var model = graphSageModelCatalog.get(
            modeParameters.username(),
            modeParameters.modelName()
        );

        var algorithm = new GraphSage(
            graph,
            model,
            parameters.algorithmParameters().concurrency(),
            parameters.algorithmParameters().batchSize(),
            DefaultPool.INSTANCE,
            progressTracker,
            terminationFlag
        );

        return algorithmMachinery.runAlgorithmsAndManageProgressTracker(
            algorithm,
            progressTracker,
            true,
            parameters.algorithmParameters().concurrency()
        );
    }


    public Model<ModelData, GraphSageTrainConfig, GraphSageModelTrainer.GraphSageTrainMetrics> graphSageTrain(
        Graph graph,
        GraphSageTrainParameters parameters,
        GraphSageTrainConfig config,
        ProgressTracker progressTracker
    ) {
        var algorithm = graphSageTrainAlgorithmFactory.create(
            graph,
            config,
            parameters,
            progressTracker,
            terminationFlag
        );

        return algorithmMachinery.runAlgorithmsAndManageProgressTracker(
            algorithm,
            progressTracker,
            true,
            parameters.concurrency()
        );
    }

    public HashGNNResult hashGnn(Graph graph, HashGNNParameters  parameters, ProgressTracker progressTracker) {

        var algorithm = new HashGNN(graph, parameters, progressTracker, terminationFlag);

        return algorithmMachinery.runAlgorithmsAndManageProgressTracker(
            algorithm,
            progressTracker,
            true,
            parameters.concurrency()
        );
    }

    Node2VecResult node2Vec(Graph graph, Node2VecParameters parameters, ProgressTracker progressTracker) {

        var algorithm =  Node2Vec.create(
            graph,
            parameters,
            progressTracker,
            terminationFlag
        );

        return algorithmMachinery.runAlgorithmsAndManageProgressTracker(
            algorithm,
            progressTracker,
            true,
            parameters.concurrency()
        );
    }

}

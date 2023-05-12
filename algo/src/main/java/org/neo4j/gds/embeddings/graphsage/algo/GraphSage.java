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
package org.neo4j.gds.embeddings.graphsage.algo;

import org.neo4j.gds.Algorithm;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.model.Model;
import org.neo4j.gds.core.utils.paged.HugeObjectArray;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.embeddings.graphsage.GraphSageEmbeddingsGenerator;
import org.neo4j.gds.embeddings.graphsage.GraphSageHelper;
import org.neo4j.gds.embeddings.graphsage.GraphSageModelTrainer;
import org.neo4j.gds.embeddings.graphsage.Layer;
import org.neo4j.gds.embeddings.graphsage.ModelData;

import java.util.concurrent.ExecutorService;

import static org.neo4j.gds.embeddings.graphsage.GraphSageHelper.initializeMultiLabelFeatures;
import static org.neo4j.gds.embeddings.graphsage.GraphSageHelper.initializeSingleLabelFeatures;

public class GraphSage extends Algorithm<GraphSageResult> {

    public static final String MODEL_TYPE = "graphSage";

    private final Graph graph;
    private final GraphSageBaseConfig config;
    private final Model<ModelData, GraphSageTrainConfig, GraphSageModelTrainer.GraphSageTrainMetrics> model;
    private final ExecutorService executor;

    public GraphSage(
        Graph graph,
        Model<ModelData, GraphSageTrainConfig, GraphSageModelTrainer.GraphSageTrainMetrics> model,
        GraphSageBaseConfig config,
        ExecutorService executor,
        ProgressTracker progressTracker
    ) {
        super(progressTracker);
        this.graph = graph;
        this.config = config;
        this.model = model;
        this.executor = executor;
    }

    @Override
    public GraphSageResult compute() {
        Layer[] layers = model.data().layers();

        var embeddingsGenerator = new GraphSageEmbeddingsGenerator(
            layers,
            config.batchSize(),
            config.concurrency(),
            model.data().featureFunction(),
            model.trainConfig().randomSeed(),
            executor,
            progressTracker
        );

        GraphSageTrainConfig trainConfig = model.trainConfig();

        var features = trainConfig.isMultiLabel() ?
            initializeMultiLabelFeatures(
                graph,
                GraphSageHelper.multiLabelFeatureExtractors(graph, trainConfig)
            )
            : initializeSingleLabelFeatures(graph, trainConfig);

        HugeObjectArray<double[]> embeddings = embeddingsGenerator.makeEmbeddings(
            graph,
            features
        );
        return GraphSageResult.of(embeddings);
    }
}

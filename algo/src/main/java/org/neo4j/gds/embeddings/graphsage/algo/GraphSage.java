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
import org.neo4j.gds.embeddings.graphsage.GraphSageEmbeddingsGenerator;
import org.neo4j.gds.embeddings.graphsage.GraphSageHelper;
import org.neo4j.gds.embeddings.graphsage.Layer;
import org.neo4j.gds.embeddings.graphsage.ModelData;
import org.neo4j.graphalgo.annotation.ValueClass;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.model.Model;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeObjectArray;
import org.neo4j.graphalgo.core.utils.progress.v2.tasks.ProgressTracker;

import java.util.concurrent.ExecutorService;

import static org.neo4j.gds.embeddings.graphsage.GraphSageHelper.initializeMultiLabelFeatures;
import static org.neo4j.gds.embeddings.graphsage.GraphSageHelper.initializeSingleLabelFeatures;

public class GraphSage extends Algorithm<GraphSage, GraphSage.GraphSageResult> {

    public static final String MODEL_TYPE = "graphSage";

    private final Graph graph;
    private final GraphSageBaseConfig config;
    private final Model<ModelData, GraphSageTrainConfig> model;
    private final ExecutorService executor;
    private final AllocationTracker tracker;

    public GraphSage(
        Graph graph,
        GraphSageBaseConfig config,
        ExecutorService executor,
        AllocationTracker tracker,
        ProgressTracker progressTracker
    ) {
        this.graph = graph;
        this.config = config;
        this.model = config.model();
        this.executor = executor;
        this.tracker = tracker;
        this.progressTracker = progressTracker;
    }

    @Override
    public GraphSageResult compute() {
        Layer[] layers = model.data().layers();
        GraphSageEmbeddingsGenerator embeddingsGenerator = new GraphSageEmbeddingsGenerator(
            layers,
            config.batchSize(),
            config.concurrency(),
            model.trainConfig().isWeighted(),
            model.data().featureFunction(),
            executor,
            progressTracker,
            tracker
        );

        GraphSageTrainConfig trainConfig = model.trainConfig();

        var features = trainConfig.isMultiLabel() ?
            initializeMultiLabelFeatures(
                graph,
                GraphSageHelper.multiLabelFeatureExtractors(graph, trainConfig), tracker
            )
            : initializeSingleLabelFeatures(graph, trainConfig, tracker);

        HugeObjectArray<double[]> embeddings = embeddingsGenerator.makeEmbeddings(
            graph,
            features
        );
        return GraphSageResult.of(embeddings);
    }

    @Override
    public GraphSage me() {
        return this;
    }

    @Override
    public void release() {

    }

    @ValueClass
    public
    interface GraphSageResult {
        HugeObjectArray<double[]> embeddings();

        static GraphSageResult of(HugeObjectArray<double[]> embeddings) {
            return ImmutableGraphSageResult.of(embeddings);
        }
    }
}

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

import org.neo4j.gds.embeddings.graphsage.GraphSageEmbeddingsGenerator;
import org.neo4j.gds.embeddings.graphsage.GraphSageTrainModel;
import org.neo4j.graphalgo.annotation.ValueClass;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.utils.paged.HugeObjectArray;
import org.neo4j.logging.Log;

import java.util.Map;

public class GraphSage extends GraphSageBase<GraphSage, GraphSage.GraphSageResult, GraphSageBaseConfig> {

    private final GraphSageBaseConfig config;

    private final GraphSageTrainModel trainModel;

    public GraphSage(Graph graph, GraphSageBaseConfig config, Log log) {
        super(graph, config);

        this.trainModel = new GraphSageTrainModel(config, log);
        this.config = config;
    }

    @Override
    public GraphSageResult compute() {
        // TODO: Split training into its own procedure?
        HugeObjectArray<double[]> features = initializeFeatures();
        GraphSageTrainModel.ModelTrainResult trainResult = trainModel.train(graph, features);
        GraphSageEmbeddingsGenerator embeddingsGenerator = new GraphSageEmbeddingsGenerator(
            trainResult.layers(),
            config.batchSize(),
            config.concurrency()
        );
        HugeObjectArray<double[]> embeddings = embeddingsGenerator.makeEmbeddings(graph, features);
        return GraphSageResult.of(trainResult.startLoss(), trainResult.epochLosses(), embeddings);
    }

    @Override
    public GraphSage me() {
        return this;
    }

    @Override
    public void release() {

    }

    @ValueClass
    public interface GraphSageResult {

        Map<String, Double> epochLosses();

        HugeObjectArray<double[]> embeddings();

        double startLoss();

        static GraphSageResult of(
            double startLoss,
            Map<String, Double> epochLosses,
            HugeObjectArray<double[]> embeddings
        ) {
            return ImmutableGraphSageResult.builder()
                .startLoss(startLoss)
                .epochLosses(epochLosses)
                .embeddings(embeddings)
                .build();
        }
    }
}

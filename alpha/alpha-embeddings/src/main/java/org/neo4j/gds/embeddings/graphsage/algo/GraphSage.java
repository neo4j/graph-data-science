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
import org.neo4j.graphalgo.Algorithm;
import org.neo4j.graphalgo.annotation.ValueClass;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.model.Model;
import org.neo4j.graphalgo.core.model.ModelCatalog;
import org.neo4j.graphalgo.core.utils.paged.HugeObjectArray;

import static org.neo4j.gds.embeddings.graphsage.GraphSageHelper.initializeFeatures;

public class GraphSage extends Algorithm<GraphSage, GraphSage.GraphSageResult> {

    public static final String MODEL_TYPE = "graphSage";

    private final Graph graph;
    private final GraphSageBaseConfig config;

    public GraphSage(Graph graph, GraphSageBaseConfig config) {
        this.graph = graph;
        this.config = config;
    }

    @Override
    public GraphSageResult compute() {
        Model<GraphSageModel> model = ModelCatalog.get(config.modelName());
        GraphSageModel graphSageModel = model.data();
        GraphSageEmbeddingsGenerator embeddingsGenerator = new GraphSageEmbeddingsGenerator(
            graphSageModel.layers(),
            config.batchSize(),
            config.concurrency()
        );

        HugeObjectArray<double[]> embeddings = embeddingsGenerator.makeEmbeddings(
            graph,
            initializeFeatures(
                graph,
                graphSageModel.nodePropertyNames(),
                graphSageModel.useDegreeAsProperty()
            )
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

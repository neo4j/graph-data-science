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
package org.neo4j.gds.embeddings.graphsage;

import org.neo4j.gds.embeddings.graphsage.ddl4j.ComputationContext;
import org.neo4j.gds.embeddings.graphsage.ddl4j.Variable;
import org.neo4j.gds.embeddings.graphsage.ddl4j.tensor.Matrix;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeObjectArray;

import java.util.Arrays;

import static org.neo4j.graphalgo.core.concurrency.ParallelUtil.parallelStreamConsume;

public class GraphSageEmbeddingsGenerator extends GraphSageBase {
    private final Layer[] layers;
    private final BatchProvider batchProvider;
    private final int concurrency;

    public GraphSageEmbeddingsGenerator(
        Layer[] layers,
        int batchSize,
        int concurrency
    ) {
        this.layers = layers;
        this.batchProvider = new BatchProvider(batchSize);
        this.concurrency = concurrency;
    }

    public HugeObjectArray<double[]> makeEmbeddings(Graph graph, HugeObjectArray<double[]> features) {
        HugeObjectArray<double[]> result = HugeObjectArray.newArray(
            double[].class,
            graph.nodeCount(),
            AllocationTracker.EMPTY
        );

        parallelStreamConsume(
            batchProvider.stream(graph),
            concurrency,
            batches -> batches.forEach(batch -> {
                ComputationContext ctx = new ComputationContext();
                Variable<Matrix> embeddingVariable = embeddingVariable(graph, batch, features, this.layers);
                int cols = embeddingVariable.dimension(1);
                double[] embeddings = ctx.forward(embeddingVariable).data();

                for (int nodeIndex = 0; nodeIndex < batch.length; nodeIndex++) {
                    double[] nodeEmbedding = Arrays.copyOfRange(
                        embeddings,
                        nodeIndex * cols,
                        (nodeIndex + 1) * cols
                    );
                    result.set(batch[nodeIndex], nodeEmbedding);
                }
            })
        );

        return result;
    }
}

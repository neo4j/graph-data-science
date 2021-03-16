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
package org.neo4j.gds.embeddings.graphsage;

import org.neo4j.gds.embeddings.graphsage.ddl4j.Variable;
import org.neo4j.gds.embeddings.graphsage.ddl4j.functions.LabelwiseFeatureProjection;
import org.neo4j.gds.embeddings.graphsage.ddl4j.functions.Weights;
import org.neo4j.gds.embeddings.graphsage.ddl4j.tensor.Matrix;
import org.neo4j.gds.embeddings.graphsage.ddl4j.tensor.Tensor;
import org.neo4j.graphalgo.NodeLabel;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.NodeMapping;
import org.neo4j.graphalgo.core.utils.paged.HugeObjectArray;

import java.util.Map;

public class MultiLabelFeatureFunction implements FeatureFunction {

    private final Map<NodeLabel, Weights<? extends Tensor<?>>> weightsByLabel;
    private final int projectedFeatureDimension;

    public MultiLabelFeatureFunction(Map<NodeLabel, Weights<? extends Tensor<?>>> weightsByLabel, int projectedFeatureDimension) {
        this.weightsByLabel = weightsByLabel;
        this.projectedFeatureDimension = projectedFeatureDimension;
    }

    public Map<NodeLabel, Weights<? extends Tensor<?>>> weightsByLabel() {
        return this.weightsByLabel;
    }

    /**
     * This method expects the graph to be validated beforehand, such that each node has exactly one label
     * See feature initialization in {@link GraphSageHelper}.
     *
     * @param nodeIds batch of node IDs
     * @param features the global property array
     * @return Create a matrix variable around a batch of nodes.
     */
    @Override
    public Variable<Matrix> apply(Graph graph, long[] nodeIds, HugeObjectArray<double[]> features) {
        var labels = new NodeLabel[nodeIds.length];
        var consumer = new SingleNodeLabelConsumer();

        for (int i = 0; i < nodeIds.length; i++) {
            graph.forEachNodeLabel(nodeIds[i], consumer);
            labels[i] = consumer.nodeLabel;
        }
        return new LabelwiseFeatureProjection(nodeIds, features, weightsByLabel, projectedFeatureDimension, labels);
    }

    private static class SingleNodeLabelConsumer implements NodeMapping.NodeLabelConsumer {

        NodeLabel nodeLabel;

        @Override
        public boolean accept(NodeLabel nodeLabel) {
            this.nodeLabel = nodeLabel;
            return false;
        }
    }

    public int projectedFeatureDimension() {
        return projectedFeatureDimension;
    }
}

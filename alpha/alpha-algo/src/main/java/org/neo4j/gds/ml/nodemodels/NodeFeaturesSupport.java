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
package org.neo4j.gds.ml.nodemodels;

import org.neo4j.gds.embeddings.graphsage.ddl4j.functions.MatrixConstant;
import org.neo4j.gds.ml.Batch;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.NodeProperties;

import java.util.List;

public final class NodeFeaturesSupport {

    private NodeFeaturesSupport() {}

    public static MatrixConstant features(Graph graph, Batch batch, List<String> nodePropertyKeys) {
        int rows = batch.size();
        int cols = nodePropertyKeys.size() + 1;
        double[] features = new double[rows * cols];
        for (int j = 0; j < nodePropertyKeys.size(); j++) {
            NodeProperties nodeProperties = graph.nodeProperties(nodePropertyKeys.get(j));
            int nodeOffset = 0;
            for (long nodeId : batch.nodeIds()) {
                features[nodeOffset * cols + j] = nodeProperties.doubleValue(nodeId);
                nodeOffset++;
            }
        }
        for (int nodeOffset = 0; nodeOffset < batch.size(); nodeOffset++) {
            features[nodeOffset * cols + cols - 1] = 1.0;
        }
        return new MatrixConstant(features, rows, cols);
    }
}

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
package org.neo4j.gds.beta.node2vec;

import org.neo4j.gds.api.properties.nodes.FloatArrayNodePropertyValues;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.core.utils.paged.HugeObjectArray;
import org.neo4j.gds.embeddings.node2vec.Node2Vec;
import org.neo4j.gds.embeddings.node2vec.Node2VecBaseConfig;
import org.neo4j.gds.embeddings.node2vec.Node2VecModel;
import org.neo4j.gds.executor.ComputationResult;
import org.neo4j.gds.ml.core.tensor.FloatVector;

final class Node2VecCompanion {

    static final String DESCRIPTION = "The Node2Vec algorithm computes embeddings for nodes based on random walks.";

    static <CONFIG extends Node2VecBaseConfig> NodePropertyValues nodeProperties(
        ComputationResult<Node2Vec, Node2VecModel.Result, CONFIG> computationResult
    ) {
        var size = computationResult.graph().nodeCount();
        HugeObjectArray<FloatVector> embeddings = computationResult.result().embeddings();

        return new FloatArrayNodePropertyValues() {
            @Override
            public long valuesStored() {
                return size;
            }

            @Override
            public float[] floatArrayValue(long nodeId) {
                return embeddings.get(nodeId).data();
            }
        };
    }

    private Node2VecCompanion() {}
}

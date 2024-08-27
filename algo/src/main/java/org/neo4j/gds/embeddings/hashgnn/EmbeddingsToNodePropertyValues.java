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
package org.neo4j.gds.embeddings.hashgnn;

import org.neo4j.gds.api.properties.nodes.BinaryArrayNodePropertyValues;
import org.neo4j.gds.api.properties.nodes.DoubleArrayNodePropertyValues;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.collections.ha.HugeObjectArray;
import org.neo4j.gds.core.utils.paged.HugeAtomicBitSet;

public final class EmbeddingsToNodePropertyValues {
    private EmbeddingsToNodePropertyValues() {}

    static DoubleArrayNodePropertyValues fromDense(HugeObjectArray<double[]> denseEmbeddings) {
        return new DoubleArrayNodePropertyValues(){
            @Override
            public double[] doubleArrayValue(long nodeId) {
                return denseEmbeddings.get(nodeId);
            }

            @Override
            public long nodeCount() {
                return denseEmbeddings.size();
            }
        };
    }

    static NodePropertyValues fromBinary(HugeObjectArray<HugeAtomicBitSet> binaryEmbeddings, int embeddingDimension) {
        return new BinaryArrayNodePropertyValues(binaryEmbeddings, embeddingDimension);
    }
}

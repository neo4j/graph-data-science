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

import org.neo4j.gds.api.properties.nodes.DoubleArrayNodePropertyValues;
import org.neo4j.gds.core.utils.paged.HugeObjectArray;

class EmbeddingNodePropertyValues implements DoubleArrayNodePropertyValues {
    private final long size;
    private final HugeObjectArray<double[]> embeddings;

    EmbeddingNodePropertyValues(HugeObjectArray<double[]> embeddings) {
        this.embeddings = embeddings;
        this.size = embeddings.size();
    }

    @Override
    public long nodeCount() {
        return size;
    }

    @Override
    public double[] doubleArrayValue(long nodeId) {

        return embeddings.get(nodeId);
    }
}

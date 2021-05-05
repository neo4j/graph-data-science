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
package org.neo4j.gds.core.ml.features;

import org.neo4j.gds.core.ml.EmbeddingUtils;
import org.neo4j.graphalgo.api.Graph;

public class ArrayPropertyExtractor implements ArrayFeatureExtractor {
    private final int dimension;
    private final Graph graph;
    private final String propertyKey;

    ArrayPropertyExtractor(int dimension, Graph graph, String propertyKey) {
        this.dimension = dimension;
        this.graph = graph;
        this.propertyKey = propertyKey;
    }

    @Override
    public int dimension() {
        return dimension;
    }

    @Override
    public double[] extract(long nodeId) {
        return EmbeddingUtils.getCheckedDoubleArrayNodeProperty(graph, propertyKey, nodeId, dimension);
    }
}

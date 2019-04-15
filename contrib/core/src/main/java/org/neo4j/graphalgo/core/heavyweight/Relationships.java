/*
 * Copyright (c) 2017 "Neo4j, Inc." <http://neo4j.com>
 *
 * This file is part of Neo4j Graph Algorithms <http://github.com/neo4j-contrib/neo4j-graph-algorithms>.
 *
 * Neo4j Graph Algorithms is free software: you can redistribute it and/or modify
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
package org.neo4j.graphalgo.core.heavyweight;

import org.neo4j.graphalgo.api.WeightMapping;
import org.neo4j.graphalgo.core.NullWeightMap;
import org.neo4j.graphalgo.core.WeightMap;

class Relationships {
    private final long offset;
    private final long rows;
    private final AdjacencyMatrix matrix;
    private final WeightMap relWeights;
    private final double defaultWeight;

    Relationships(long offset, long rows, AdjacencyMatrix matrix, WeightMap relWeights, double defaultWeight) {
        this.offset = offset;
        this.rows = rows;
        this.matrix = matrix;
        this.relWeights = relWeights;
        this.defaultWeight = defaultWeight;
    }

    WeightMapping weights() {
        if (relWeights != null) {
            return relWeights;
        }
        return new NullWeightMap(defaultWeight);
    }

    public AdjacencyMatrix matrix() {
        return matrix;
    }

    public long rows() {
        return rows;
    }

    public long offset() {
        return offset;
    }

    public WeightMap relWeights() {
        return relWeights;
    }
}

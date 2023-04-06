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
package org.neo4j.gds.similarity.nodesim;

import org.neo4j.gds.functions.similairty.OverlapSimilarity;

class OverlapSimilarityComputer implements MetricSimilarityComputer {
    private final double similarityCutoff;

    private OverlapSimilarityComputer(double similarityCutoff) {
        this.similarityCutoff = similarityCutoff;
    }

    @Override
    public double computeSimilarity(long[] vector1, long[] vector2) {
        return OverlapSimilarity.computeSimilarity(vector1, vector2, similarityCutoff);
    }

    @Override
    public double computeWeightedSimilarity(long[] vector1, long[] vector2, double[] weights1, double[] weights2) {
        return OverlapSimilarity.computeWeightedSimilarity(vector1, vector2, weights1, weights2, similarityCutoff);
    }

    static class Builder implements MetricSimilarityComputerBuilder {
        @Override
        public MetricSimilarityComputer build(double similarityCutoff) {
            return new OverlapSimilarityComputer(similarityCutoff);
        }

        @Override
        public String render() {
            return "OVERLAP";
        }
    }
}

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
package org.neo4j.gds.similarity.knn.metrics;

import org.neo4j.gds.api.Graph;
import org.neo4j.gds.similarity.knn.KnnNodePropertySpec;

import java.util.Arrays;
import java.util.List;

final class CombinedSimilarityComputer implements SimilarityComputer {
    private final SimilarityComputer[] similarityComputers;
    private final int numOfProperties;
    private final boolean isSymmetric;

    CombinedSimilarityComputer(Graph graph, List<KnnNodePropertySpec> propertyNames) {
        this.numOfProperties = propertyNames.size();
        this.similarityComputers = new SimilarityComputer[numOfProperties];

        for (int i = 0; i < numOfProperties; ++i) {
            this.similarityComputers[i] = SimilarityComputer.ofProperty(graph, propertyNames.get(i));
        }

        this.isSymmetric = Arrays.stream(similarityComputers).allMatch(SimilarityComputer::isSymmetric);
    }

    @Override
    public double similarity(long firstNodeId, long secondNodeId) {
        var sum = 0D;
        for (var similarityComputer : similarityComputers) {
            sum += similarityComputer.safeSimilarity(firstNodeId, secondNodeId);
        }
        return sum / numOfProperties;
    }

    @Override
    public boolean isSymmetric() {
        return isSymmetric;
    }
}

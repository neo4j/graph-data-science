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
package org.neo4j.gds.similarity.knn;

import org.neo4j.gds.similarity.knn.metrics.SimilarityMetric;

public class KnnNodePropertySpec {
    private final String propertyName;
    private SimilarityMetric similarityMetric;

    public KnnNodePropertySpec(String propertyName) {
        this(propertyName, SimilarityMetric.DEFAULT);
    }

    KnnNodePropertySpec(String propertyName, SimilarityMetric similarityMetric) {
        this.propertyName = propertyName;
        this.similarityMetric = similarityMetric;
    }

    public String name() {
        return propertyName;
    }

    public SimilarityMetric metric() {
        return similarityMetric;
    }

    // Once we are able to look up the actual default value, in the Similarity Computer,
    // we set that value here. This allows us to include the actual metric used when returning
    // the config to the user.
    public void setMetric(SimilarityMetric similarityMetric) {
        this.similarityMetric = similarityMetric;
    }
}

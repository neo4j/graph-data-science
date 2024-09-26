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

import java.util.Locale;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public interface MetricSimilarityComputer {
    double computeSimilarity(long[] vector1, long[] vector2);

    double computeWeightedSimilarity(long[] vector1, long[] vector2, double[] weights1, double[] weights2);

    static MetricSimilarityComputerBuilder parse(Object userInput) {
        if (userInput instanceof MetricSimilarityComputerBuilder) {
            return (MetricSimilarityComputerBuilder) userInput;
        }
        if (userInput instanceof String) {
            return create(valueOf((String) userInput));
        }
        throw new IllegalArgumentException(
            formatWithLocale("Unsupported input type: Expected String but received %s.", userInput.getClass().getSimpleName())
        );
    }

    static String render(MetricSimilarityComputerBuilder input) {
        return input.render();
    }

    private static NodeSimilarityMetric valueOf(String userInput) {
        String userInputInCaps = userInput.toUpperCase(Locale.ROOT);
        if (userInputInCaps.equals("JACCARD")) {
            return NodeSimilarityMetric.JACCARD;
        } else if (userInputInCaps.equals("OVERLAP")) {
            return NodeSimilarityMetric.OVERLAP;
        } else if (userInputInCaps.equals("COSINE")) {
            return NodeSimilarityMetric.COSINE;
        } else {
            throw new IllegalArgumentException(userInput + " is not a valid metric. Available metrics include Jaccard and Overlap");
        }
    }

    private static MetricSimilarityComputerBuilder create(NodeSimilarityMetric metric) {
        if (metric == NodeSimilarityMetric.JACCARD) {
            return new JaccardSimilarityComputer.Builder();
        } else if (metric == NodeSimilarityMetric.OVERLAP) {
            return new OverlapSimilarityComputer.Builder();
        } else {
            return new CosineSimilarityComputer.Builder();
        }

    }

    interface MetricSimilarityComputerBuilder {
        MetricSimilarityComputer build(double similarityCutoff);
        String render();
    }
}

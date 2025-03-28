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

public interface NodeSimilarityMetricParser {

    static NodeSimilarityMetric parse(Object userInput) {
        if (userInput instanceof  NodeSimilarityMetric ){
            return (NodeSimilarityMetric) userInput;
        }
        else if (userInput instanceof String) {
            return valueOf((String) userInput);
        }
        throw new IllegalArgumentException(
            formatWithLocale("Unsupported input type: Expected String but received %s.", userInput.getClass().getSimpleName())
        );
    }


    private static NodeSimilarityMetric valueOf(String userInput) {
        String userInputInCaps = userInput.toUpperCase(Locale.ROOT);
        return switch (userInputInCaps) {
            case "JACCARD" -> NodeSimilarityMetric.JACCARD;
            case "OVERLAP" -> NodeSimilarityMetric.OVERLAP;
            case "COSINE" -> NodeSimilarityMetric.COSINE;
            default ->
                throw new IllegalArgumentException(userInput + " is not a valid metric. Available metrics include Jaccard and Overlap");
        };
    }

    static String toString(NodeSimilarityMetric metric) {
        return metric.toString();
    }

}

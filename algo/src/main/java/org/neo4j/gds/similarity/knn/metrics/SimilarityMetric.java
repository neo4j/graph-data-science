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

import org.neo4j.gds.api.nodeproperties.ValueType;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;
import static org.neo4j.gds.utils.StringFormatting.toUpperCaseWithLocale;

public enum SimilarityMetric {
    JACCARD, OVERLAP, COSINE, EUCLIDEAN, PEARSON, LONG_PROPERTY_METRIC, DOUBLE_PROPERTY_METRIC, DEFAULT;

    public static SimilarityMetric parse(String value) {
        return SimilarityMetric.valueOf(toUpperCaseWithLocale(value));
    }

    public static SimilarityMetric defaultMetricForType(ValueType valueType) {
        switch (valueType) {
            case LONG:
                return LONG_PROPERTY_METRIC;
            case DOUBLE:
                return DOUBLE_PROPERTY_METRIC;
            case DOUBLE_ARRAY:
            case FLOAT_ARRAY:
                return COSINE;
            case LONG_ARRAY:
                return JACCARD;
            default:
                throw new IllegalArgumentException(
                    formatWithLocale(
                        "No default similarity metric exists for value type [%s].",
                        valueType
                    )
                );
        }
    }
}

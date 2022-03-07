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
import org.neo4j.gds.api.NodeProperties;
import org.neo4j.gds.api.NodePropertyContainer;
import org.neo4j.gds.api.nodeproperties.ValueType;
import org.neo4j.gds.similarity.knn.metrics.LongArrayPropertySimilarityComputer.SortedLongArrayProperties;

import java.util.List;
import java.util.Objects;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public interface SimilarityComputer {

    default double safeSimilarity(long firstNodeId, long secondNodeId) {
        double similarity = similarity(firstNodeId, secondNodeId);
        return Double.isFinite(similarity) ? similarity : 0.0;
    }

    double similarity(long firstNodeId, long secondNodeId);

    static SimilarityComputer ofProperties(Graph graph, List<String> propertyNames) {
        if (propertyNames.size() == 1) {
            return ofProperty((graph), propertyNames.get(0));
        }
        return new CombinedSimilarityComputer(graph, propertyNames);
    }

    static SimilarityComputer ofProperty(NodePropertyContainer graph, String propertyName) {
        var nodeProperties = Objects.requireNonNull(
            graph.nodeProperties(propertyName),
            () -> formatWithLocale("The property `%s` has not been loaded", propertyName)
        );
        return ofProperty(nodeProperties, propertyName);
    }

    static SimilarityComputer ofProperty(NodeProperties nodeProperties, String propertyName) {
        return ofProperty(nodeProperties, propertyName, SimilarityMetric.defaultMetricForType(nodeProperties.valueType()));
    }

    static SimilarityComputer ofProperty(
        NodeProperties nodeProperties,
        String propertyName,
        SimilarityMetric defaultSimilarityMetric
    ) {
        switch (nodeProperties.valueType()) {
            case LONG:
                return ofLongProperty(nodeProperties);
            case DOUBLE:
                return ofDoubleProperty(nodeProperties);
            case DOUBLE_ARRAY:
                return ofDoubleArrayProperty(nodeProperties, defaultSimilarityMetric);
            case FLOAT_ARRAY:
                return ofFloatArrayProperty(nodeProperties, defaultSimilarityMetric);
            case LONG_ARRAY:
                return ofLongArrayProperty(new SortedLongArrayProperties(nodeProperties), defaultSimilarityMetric);
            default:
                throw new IllegalArgumentException(formatWithLocale(
                    "The property [%s] has an unsupported type [%s].",
                    propertyName,
                    nodeProperties.valueType()
                ));
        }
    }

    static SimilarityComputer ofDoubleProperty(NodeProperties nodeProperties) {
        return new DoublePropertySimilarityComputer(nodeProperties);
    }

    static SimilarityComputer ofLongProperty(NodeProperties nodeProperties) {
        return new LongPropertySimilarityComputer(nodeProperties);
    }

    static SimilarityComputer ofFloatArrayProperty(NodeProperties nodeProperties, SimilarityMetric similarityMetric) {
        switch (similarityMetric) {
            case COSINE:
                return new FloatArrayPropertySimilarityComputer(nodeProperties, Cosine::floatMetric);
            default:
                throw unsupportedSimilarityMetric(nodeProperties.valueType(), similarityMetric);
        }
    }

    static SimilarityComputer ofDoubleArrayProperty(NodeProperties nodeProperties, SimilarityMetric similarityMetric) {
        switch (similarityMetric) {
            case COSINE:
                return new DoubleArrayPropertySimilarityComputer(nodeProperties, Cosine::doubleMetric);
            default:
                throw unsupportedSimilarityMetric(nodeProperties.valueType(), similarityMetric);
        }
    }

    static SimilarityComputer ofLongArrayProperty(NodeProperties nodeProperties, SimilarityMetric similarityMetric) {
        switch (similarityMetric) {
            case JACCARD:
                return new LongArrayPropertySimilarityComputer(nodeProperties, Jaccard::metric);
            case OVERLAP:
                return new LongArrayPropertySimilarityComputer(nodeProperties, Overlap::metric);
            default:
                throw unsupportedSimilarityMetric(nodeProperties.valueType(), similarityMetric);
        }
    }

    static IllegalArgumentException unsupportedSimilarityMetric(
        ValueType valueType,
        SimilarityMetric similarityMetric
    ) {
        return new IllegalArgumentException(
            formatWithLocale(
                "Similarity metric [%s] is not supported for property type [%s].",
                similarityMetric,
                valueType
            )
        );
    }
}

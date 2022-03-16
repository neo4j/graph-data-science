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
import org.neo4j.gds.similarity.knn.KnnNodePropertySpec;
import org.neo4j.gds.similarity.knn.metrics.LongArrayPropertySimilarityComputer.SortedLongArrayProperties;

import java.util.List;
import java.util.Objects;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public interface SimilarityComputer {

    default double safeSimilarity(long firstNodeId, long secondNodeId) {
        double similarity = similarity(firstNodeId, secondNodeId);
        return Double.isFinite(similarity) ? similarity : 0.0;
    }

    default void throwForNode(long nodeId, String propertyName) {
        throw new IllegalArgumentException(formatWithLocale(
            "Missing node property `%s` for node with id `%s`.",
            propertyName,
            nodeId
        ));
    }

    double similarity(long firstNodeId, long secondNodeId);

    static SimilarityComputer ofProperties(Graph graph, List<KnnNodePropertySpec> knnNodeProperties) {
        if (knnNodeProperties.size() == 1) {
            return ofProperty(graph, knnNodeProperties.get(0));
        }
        return new CombinedSimilarityComputer(graph, knnNodeProperties);
    }

    static SimilarityComputer ofProperty(NodePropertyContainer graph, KnnNodePropertySpec knnNodePropertySpec) {
        var propertyName = knnNodePropertySpec.name();
        var nodeProperties = Objects.requireNonNull(
            graph.nodeProperties(propertyName),
            () -> formatWithLocale("The property `%s` has not been loaded", propertyName)
        );

        if (knnNodePropertySpec.metric() == SimilarityMetric.DEFAULT) {
            knnNodePropertySpec.setMetric(SimilarityMetric.defaultMetricForType(nodeProperties.valueType()));
        }
        return ofProperty(propertyName, nodeProperties, knnNodePropertySpec.metric());
    }

    static SimilarityComputer ofProperty(String propertyName, NodeProperties nodeProperties) {
        return ofProperty(propertyName, nodeProperties, SimilarityMetric.defaultMetricForType(nodeProperties.valueType()));
    }

    static SimilarityComputer ofProperty(
        String name,
        NodeProperties properties,
        SimilarityMetric defaultSimilarityMetric
    ) {
        switch (properties.valueType()) {
            case LONG:
                return ofLongProperty(properties);
            case DOUBLE:
                return ofDoubleProperty(properties);
            case DOUBLE_ARRAY:
                return ofDoubleArrayProperty(name, properties, defaultSimilarityMetric);
            case FLOAT_ARRAY:
                return ofFloatArrayProperty(name, properties, defaultSimilarityMetric);
            case LONG_ARRAY:
                return ofLongArrayProperty(name, new SortedLongArrayProperties(properties), defaultSimilarityMetric);
            default:
                throw new IllegalArgumentException(formatWithLocale(
                    "The property [%s] has an unsupported type [%s].",
                    name,
                    properties.valueType()
                ));
        }
    }

    static SimilarityComputer ofDoubleProperty(NodeProperties nodeProperties) {
        return new DoublePropertySimilarityComputer(nodeProperties);
    }

    static SimilarityComputer ofLongProperty(NodeProperties nodeProperties) {
        return new LongPropertySimilarityComputer(nodeProperties);
    }

    static SimilarityComputer ofFloatArrayProperty(String name, NodeProperties properties, SimilarityMetric metric) {
        switch (metric) {
            case COSINE:
                return new FloatArrayPropertySimilarityComputer(name, properties, Cosine::floatMetric);
            case EUCLIDEAN:
                return new FloatArrayPropertySimilarityComputer(name, properties, Euclidean::floatMetric);
            case PEARSON:
                return new FloatArrayPropertySimilarityComputer(name, properties, Pearson::floatMetric);
            default:
                throw unsupportedSimilarityMetric(name, properties.valueType(), metric);
        }
    }

    static SimilarityComputer ofDoubleArrayProperty(
        String propertyName,
        NodeProperties nodeProperties,
        SimilarityMetric similarityMetric
    ) {
        switch (similarityMetric) {
            case COSINE:
                return new DoubleArrayPropertySimilarityComputer(propertyName, nodeProperties, Cosine::doubleMetric);
            case EUCLIDEAN:
                return new DoubleArrayPropertySimilarityComputer(propertyName, nodeProperties, Euclidean::doubleMetric);
            case PEARSON:
                return new DoubleArrayPropertySimilarityComputer(propertyName, nodeProperties, Pearson::doubleMetric);
            default:
                throw unsupportedSimilarityMetric(propertyName, nodeProperties.valueType(), similarityMetric);
        }
    }

    static SimilarityComputer ofLongArrayProperty(String propertyName, NodeProperties nodeProperties, SimilarityMetric similarityMetric) {
        switch (similarityMetric) {
            case JACCARD:
                return new LongArrayPropertySimilarityComputer(propertyName, nodeProperties, Jaccard::metric);
            case OVERLAP:
                return new LongArrayPropertySimilarityComputer(propertyName, nodeProperties, Overlap::metric);
            default:
                throw unsupportedSimilarityMetric(propertyName, nodeProperties.valueType(), similarityMetric);
        }
    }

    static IllegalArgumentException unsupportedSimilarityMetric(
        String propertyName,
        ValueType valueType,
        SimilarityMetric similarityMetric
    ) {
        return new IllegalArgumentException(
            formatWithLocale(
                "Similarity metric [%s] is not supported for property [%s] of type [%s].",
                propertyName,
                similarityMetric,
                valueType
            )
        );
    }
}

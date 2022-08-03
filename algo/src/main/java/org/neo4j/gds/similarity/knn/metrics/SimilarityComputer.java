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
import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.api.nodeproperties.ValueType;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.similarity.knn.KnnNodePropertySpec;
import org.neo4j.gds.similarity.knn.metrics.LongArrayPropertySimilarityComputer.SortedLongArrayPropertyValues;

import java.util.List;
import java.util.Objects;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public interface SimilarityComputer {

    default double safeSimilarity(long firstNodeId, long secondNodeId) {
        double similarity = similarity(firstNodeId, secondNodeId);
        return Double.isFinite(similarity) ? similarity : 0.0;
    }

    double similarity(long firstNodeId, long secondNodeId);

    boolean isSymmetric();

    static SimilarityComputer ofProperties(Graph graph, List<KnnNodePropertySpec> knnNodeProperties) {
        if (knnNodeProperties.size() == 1) {
            return ofProperty(graph, knnNodeProperties.get(0));
        }
        return new CombinedSimilarityComputer(graph, knnNodeProperties);
    }

    static SimilarityComputer ofProperty(Graph graph, KnnNodePropertySpec knnNodePropertySpec) {
        var propertyName = knnNodePropertySpec.name();
        var nodeProperties = Objects.requireNonNull(
            graph.nodeProperties(propertyName),
            () -> formatWithLocale("The property `%s` has not been loaded", propertyName)
        );

        if (knnNodePropertySpec.metric() == SimilarityMetric.DEFAULT) {
            knnNodePropertySpec.setMetric(SimilarityMetric.defaultMetricForType(nodeProperties.valueType()));
        }
        return ofProperty(graph, propertyName, nodeProperties, knnNodePropertySpec.metric());
    }

    static SimilarityComputer ofProperty(IdMap idMap, String propertyName, NodePropertyValues nodePropertyValues) {
        return ofProperty(idMap, propertyName,
            nodePropertyValues, SimilarityMetric.defaultMetricForType(nodePropertyValues.valueType()));
    }

    static SimilarityComputer ofProperty(
        IdMap idMap,
        String name,
        NodePropertyValues properties,
        SimilarityMetric defaultSimilarityMetric
    ) {
        switch (properties.valueType()) {
            case LONG:
                return ofLongProperty(properties);
            case DOUBLE:
                return ofDoubleProperty(properties);
            case DOUBLE_ARRAY:
                return ofDoubleArrayProperty(
                    name,
                    NullCheckingNodePropertyValues.create(properties, name, idMap),
                    defaultSimilarityMetric
                );
            case FLOAT_ARRAY:
                return ofFloatArrayProperty(
                    name,
                    NullCheckingNodePropertyValues.create(properties, name, idMap),
                    defaultSimilarityMetric
                );
            case LONG_ARRAY:
                return ofLongArrayProperty(
                    name,
                    new SortedLongArrayPropertyValues(NullCheckingNodePropertyValues.create(properties, name, idMap)),
                    defaultSimilarityMetric
                );
            default:
                throw new IllegalArgumentException(formatWithLocale(
                    "The property [%s] has an unsupported type [%s].",
                    name,
                    properties.valueType()
                ));
        }
    }

    static SimilarityComputer ofDoubleProperty(NodePropertyValues nodePropertyValues) {
        return new DoublePropertySimilarityComputer(nodePropertyValues);
    }

    static SimilarityComputer ofLongProperty(NodePropertyValues nodePropertyValues) {
        return new LongPropertySimilarityComputer(nodePropertyValues);
    }

    static SimilarityComputer ofFloatArrayProperty(String name, NodePropertyValues properties, SimilarityMetric metric) {
        switch (metric) {
            case COSINE:
                return new FloatArrayPropertySimilarityComputer(properties, Cosine::floatMetric);
            case EUCLIDEAN:
                return new FloatArrayPropertySimilarityComputer(properties, Euclidean::floatMetric);
            case PEARSON:
                return new FloatArrayPropertySimilarityComputer(properties, Pearson::floatMetric);
            default:
                throw unsupportedSimilarityMetric(name, properties.valueType(), metric);
        }
    }

    static SimilarityComputer ofDoubleArrayProperty(
        String propertyName,
        NodePropertyValues nodePropertyValues,
        SimilarityMetric similarityMetric
    ) {
        switch (similarityMetric) {
            case COSINE:
                return new DoubleArrayPropertySimilarityComputer(nodePropertyValues, Cosine::doubleMetric);
            case EUCLIDEAN:
                return new DoubleArrayPropertySimilarityComputer(nodePropertyValues, Euclidean::doubleMetric);
            case PEARSON:
                return new DoubleArrayPropertySimilarityComputer(nodePropertyValues, Pearson::doubleMetric);
            default:
                throw unsupportedSimilarityMetric(propertyName, nodePropertyValues.valueType(), similarityMetric);
        }
    }

    static SimilarityComputer ofLongArrayProperty(String propertyName, NodePropertyValues nodePropertyValues, SimilarityMetric similarityMetric) {
        switch (similarityMetric) {
            case JACCARD:
                return new LongArrayPropertySimilarityComputer(nodePropertyValues, Jaccard::metric);
            case OVERLAP:
                return new LongArrayPropertySimilarityComputer(nodePropertyValues, Overlap::metric);
            default:
                throw unsupportedSimilarityMetric(propertyName, nodePropertyValues.valueType(), similarityMetric);
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

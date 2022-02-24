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

import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.NodeProperties;
import org.neo4j.gds.api.NodePropertyContainer;
import org.neo4j.gds.api.nodeproperties.LongArrayNodeProperties;
import org.neo4j.gds.api.nodeproperties.ValueType;
import org.neo4j.gds.core.utils.Intersections;
import org.neo4j.gds.core.utils.paged.HugeObjectArray;
import org.neo4j.gds.similarity.knn.LongArrayPropertySimilarityComputer.SortedLongArrayProperties;

import java.util.Arrays;
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

    static SimilarityComputer ofProperty(NodeProperties nodeProperties, String propertyName) {
        switch (nodeProperties.valueType()) {
            case LONG:
                return ofLongProperty(nodeProperties);
            case DOUBLE:
                return ofDoubleProperty(nodeProperties);
            case DOUBLE_ARRAY:
                return ofDoubleArrayProperty(nodeProperties);
            case FLOAT_ARRAY:
                return ofFloatArrayProperty(nodeProperties);
            case LONG_ARRAY:
                return ofLongArrayProperty(new SortedLongArrayProperties(nodeProperties));
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

    static SimilarityComputer ofFloatArrayProperty(NodeProperties nodeProperties) {
        return new FloatArrayPropertySimilarityComputer(nodeProperties);
    }

    static SimilarityComputer ofDoubleArrayProperty(NodeProperties nodeProperties) {
        return new DoubleArrayPropertySimilarityComputer(nodeProperties);
    }

    static SimilarityComputer ofLongArrayProperty(NodeProperties nodeProperties) {
        return new LongArrayPropertySimilarityComputer(nodeProperties);
    }

    static SimilarityComputer ofProperty(NodePropertyContainer graph, String propertyName) {
        var nodeProperties = Objects.requireNonNull(
            graph.nodeProperties(propertyName),
            () -> formatWithLocale("The property `%s` has not been loaded", propertyName)
        );
        return ofProperty(nodeProperties, propertyName);
    }
}

final class DoublePropertySimilarityComputer implements SimilarityComputer {
    private final NodeProperties nodeProperties;

    DoublePropertySimilarityComputer(NodeProperties nodeProperties) {
        if (nodeProperties.valueType() != ValueType.DOUBLE) {
            throw new IllegalArgumentException("The property is not of type DOUBLE");
        }
        this.nodeProperties = nodeProperties;
    }

    @Override
    public double similarity(long firstNodeId, long secondNodeId) {
        var left = nodeProperties.doubleValue(firstNodeId);
        var right = nodeProperties.doubleValue(secondNodeId);
        return 1.0 / (1.0 + Math.abs(left - right));
    }
}

final class LongPropertySimilarityComputer implements SimilarityComputer {
    private final NodeProperties nodeProperties;

    LongPropertySimilarityComputer(NodeProperties nodeProperties) {
        if (nodeProperties.valueType() != ValueType.LONG) {
            throw new IllegalArgumentException("The property is not of type LONG");
        }
        this.nodeProperties = nodeProperties;
    }

    @Override
    public double similarity(long firstNodeId, long secondNodeId) {
        var left = nodeProperties.longValue(firstNodeId);
        var right = nodeProperties.longValue(secondNodeId);
        var abs = Math.abs(left - right);
        if (abs == Long.MIN_VALUE) {
            abs = Long.MAX_VALUE;
        }
        return 1.0 / (1.0 + abs);
    }
}

final class FloatArrayPropertySimilarityComputer implements SimilarityComputer {
    private final NodeProperties nodeProperties;

    FloatArrayPropertySimilarityComputer(NodeProperties nodeProperties) {
        if (nodeProperties.valueType() != ValueType.FLOAT_ARRAY) {
            throw new IllegalArgumentException("The property is not of type FLOAT_ARRAY");
        }
        this.nodeProperties = nodeProperties;
    }

    @Override
    public double similarity(long firstNodeId, long secondNodeId) {
        var left = nodeProperties.floatArrayValue(firstNodeId);
        var right = nodeProperties.floatArrayValue(secondNodeId);
        int len = Math.min(left.length, right.length);
        return Math.max(Intersections.cosine(left, right, len), 0);
    }
}

final class DoubleArrayPropertySimilarityComputer implements SimilarityComputer {
    private final NodeProperties nodeProperties;

    DoubleArrayPropertySimilarityComputer(NodeProperties nodeProperties) {
        if (nodeProperties.valueType() != ValueType.DOUBLE_ARRAY) {
            throw new IllegalArgumentException("The property is not of type DOUBLE_ARRAY");
        }
        this.nodeProperties = nodeProperties;
    }

    @Override
    public double similarity(long firstNodeId, long secondNodeId) {
        var left = nodeProperties.doubleArrayValue(firstNodeId);
        var right = nodeProperties.doubleArrayValue(secondNodeId);
        int len = Math.min(left.length, right.length);
        return Math.max(Intersections.cosine(left, right, len), 0);
    }
}

final class LongArrayPropertySimilarityComputer implements SimilarityComputer {
    private final NodeProperties nodeProperties;

    LongArrayPropertySimilarityComputer(NodeProperties nodeProperties) {
        if (nodeProperties.valueType() != ValueType.LONG_ARRAY) {
            throw new IllegalArgumentException("The property is not of type LONG_ARRAY");
        }
        this.nodeProperties = nodeProperties;
    }

    @Override
    public double similarity(long firstNodeId, long secondNodeId) {
        var left = nodeProperties.longArrayValue(firstNodeId);
        var right = nodeProperties.longArrayValue(secondNodeId);
        long sameElements = Intersections.intersection3(left, right);
        long differentElements = left.length - sameElements;
        return 1.0 / (1.0 + differentElements);
    }

    static final class SortedLongArrayProperties implements LongArrayNodeProperties {

        private final HugeObjectArray<long[]> properties;

        SortedLongArrayProperties(NodeProperties nodeProperties) {
            this.properties = HugeObjectArray.newArray(long[].class, nodeProperties.size());
            this.properties.setAll(i -> {
                var value = nodeProperties.longArrayValue(i).clone();
                Arrays.parallelSort(value);
                return value;
            });
        }

        @Override
        public long size() {
            return properties.size();
        }

        @Override
        public long[] longArrayValue(long nodeId) {
            return properties.get(nodeId);
        }
    }
}

final class CombinedSimilarityComputer implements SimilarityComputer {
    private final SimilarityComputer[] similarityComputers;
    private int numOfProperties;

    CombinedSimilarityComputer(NodePropertyContainer graph, List<String> propertyNames) {
        this.numOfProperties = propertyNames.size();
        this.similarityComputers = new SimilarityComputer[numOfProperties];

        for (int i = 0; i < numOfProperties; ++i) {
            this.similarityComputers[i] = SimilarityComputer.ofProperty(graph, propertyNames.get(i));
        }
    }

    @Override
    public double similarity(long firstNodeId, long secondNodeId) {
        var sum = 0D;
        for (var similarityComputer : similarityComputers) {
            sum += similarityComputer.safeSimilarity(firstNodeId, secondNodeId);
        }
        return sum / numOfProperties;
    }
}

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
package org.neo4j.gds.ml.linkmodels.pipeline.linkFeatures.linkfunctions;

import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.NodeProperties;
import org.neo4j.gds.ml.core.tensor.TensorFunctions;
import org.neo4j.gds.utils.StringJoining;

import java.util.List;
import java.util.stream.Stream;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public final class FeatureStepUtil {

    private FeatureStepUtil() {}

    static int totalPropertyDimension(Graph graph, List<String> nodeProperties) {
        return nodeProperties.stream().mapToInt(property -> FeatureStepUtil.propertyDimension(graph, property)).sum();
    }

    public static int propertyDimension(Graph graph, String nodeProperty) {
        var nodeProperties = graph.nodeProperties(nodeProperty);

        int dimension = 0;
        switch (nodeProperties.valueType()) {
            case LONG:
            case DOUBLE:
                dimension = 1;
                break;
            case DOUBLE_ARRAY:
            case FLOAT_ARRAY:
                dimension = nodeProperties.doubleArrayValue(0).length;
                break;
            case LONG_ARRAY:
                dimension = nodeProperties.longArrayValue(0).length;
                break;
            case UNKNOWN:
                throw new IllegalStateException(formatWithLocale("Unknown ValueType %s", nodeProperty));
        }

        return dimension;
    }

    static boolean isNaN(long nodeId, NodeProperties nodeProperty) {
        switch (nodeProperty.valueType()) {
            case DOUBLE:
                return Double.isNaN(nodeProperty.doubleValue(nodeId));
            case DOUBLE_ARRAY:
            case FLOAT_ARRAY:
                return TensorFunctions.anyMatch(nodeProperty.doubleArrayValue(nodeId), Double::isNaN);
            case UNKNOWN:
                throw new IllegalStateException(formatWithLocale("Unknown ValueType %s", nodeProperty));
            default:
                return false;
        }
    }

    static void validateComputedFeatures(double[] linkFeatures, int startOffset, int endOffset, Runnable throwError) {
        for (int offset = startOffset; offset < endOffset; offset++) {
            if (Double.isNaN(linkFeatures[offset])) {
                throwError.run();
            }
        }
    }

    static void throwNanError(
        String featureStep,
        Graph graph,
        List<String> nodeProperties,
        long source,
        long target
    ) {
        nodeProperties.forEach(propertyKey -> {
            var property = graph.nodeProperties(propertyKey);
            var nanNodes = Stream
                .of(source, target)
                .filter(node -> !isNaN(node, property))
                .map(Object::toString);

            throw new IllegalArgumentException(formatWithLocale(
                "Encountered NaN in the nodeProperty `%s` for nodes %s when computing the %s feature vector. " +
                "Either define a default value if its a stored property or check the nodePropertyStep",
                propertyKey,
                StringJoining.join(nanNodes),
                featureStep
            ));
        });
    }
}

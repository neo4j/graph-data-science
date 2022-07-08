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
package org.neo4j.gds.ml.pipeline;

import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.utils.StringJoining;

import java.util.Collection;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public final class FeatureStepUtil {

    private FeatureStepUtil() {}

    public static int propertyDimension(Graph graph, String nodeProperty) {
        return propertyDimension(graph.nodeProperties(nodeProperty), nodeProperty);
    }

    public static int propertyDimension(NodePropertyValues nodeProperties, String propertyName) {
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
                throw new IllegalStateException(formatWithLocale("Unknown ValueType %s", propertyName));
        }

        return dimension;
    }

    public static void validateComputedFeatures(
        double[] linkFeatures,
        int startOffset,
        int endOffset,
        Runnable throwError
    ) {
        for (int offset = startOffset; offset < endOffset; offset++) {
            if (Double.isNaN(linkFeatures[offset])) {
                throwError.run();
            }
        }
    }

    public static void throwNanError(
        String featureStep,
        Collection<String> nodeProperties,
        long source,
        long target
    ) {
        throw new IllegalArgumentException(formatWithLocale(
            "Encountered NaN when combining the nodeProperties %s for the node pair (%d, %d) when computing the %s feature vector. " +
            "Either define a default value if its a stored property or check the nodePropertyStep.",
            StringJoining.join(nodeProperties),
            source,
            target,
            featureStep
        ));
    }
}

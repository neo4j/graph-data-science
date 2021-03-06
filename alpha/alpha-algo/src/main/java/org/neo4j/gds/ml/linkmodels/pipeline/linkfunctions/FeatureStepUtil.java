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
package org.neo4j.gds.ml.linkmodels.pipeline.linkfunctions;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.NodeProperties;

import java.util.List;

import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;

public class FeatureStepUtil {

    static int totalPropertyDimension(Graph graph, List<String> properties) {
        return properties.stream().mapToInt(property -> FeatureStepUtil.propertyDimension(graph, property)).sum();
    }

    static int propertyDimension(Graph graph, String property) {
        NodeProperties nodeProperties = graph.nodeProperties(property);

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
                throw new IllegalStateException(formatWithLocale("Unknown ValueType %s", property));
        }

        return dimension;

    }
}

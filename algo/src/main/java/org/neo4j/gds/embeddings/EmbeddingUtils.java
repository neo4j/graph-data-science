/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
package org.neo4j.gds.embeddings;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.nodeproperties.ValueType;

import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;

public final class EmbeddingUtils {

    private EmbeddingUtils() {}

    public static double getCheckedDoubleNodeProperty(Graph graph, String propertyKey, long nodeId) {
        var nodeProperties = graph.nodeProperties(propertyKey);
        var propertyValue = Double.NaN;
        if (nodeProperties.valueType() == ValueType.DOUBLE) {
            propertyValue = nodeProperties.doubleValue(nodeId);
        }
        if (nodeProperties.valueType() == ValueType.LONG) {
            propertyValue = nodeProperties.longValue(nodeId);
        }
        if (Double.isNaN(propertyValue)) {
            throw new IllegalArgumentException(formatWithLocale(
                "Missing node property for property key `%s` on node with id `%s`. Consider using a default value in the property projection.",
                propertyKey,
                graph.toOriginalNodeId(nodeId)
            ));
        }
        return propertyValue;
    }
}

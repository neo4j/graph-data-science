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
package org.neo4j.gds.ml.core.features;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.NodeProperties;

import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;

public class ScalarPropertyExtractor implements ScalarFeatureExtractor {
    private final Graph graph;
    private final String propertyKey;
    private final NodeProperties nodeProperties;

    ScalarPropertyExtractor(Graph graph, String propertyKey) {
        this.graph = graph;
        this.propertyKey = propertyKey;
        this.nodeProperties = graph.nodeProperties(propertyKey);
    }

    @Override
    public double extract(long nodeId) {
        var propertyValue = nodeProperties.doubleValue(nodeId);
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

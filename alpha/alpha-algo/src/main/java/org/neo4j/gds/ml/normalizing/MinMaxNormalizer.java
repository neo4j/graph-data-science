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
package org.neo4j.gds.ml.normalizing;

import org.neo4j.graphalgo.api.NodeProperties;

final class MinMaxNormalizer {

    private final NodeProperties properties;
    final double min;
    final double max;

    private MinMaxNormalizer(NodeProperties properties, double min, double max) {
        this.properties = properties;
        this.min = min;
        this.max = max;
    }

    double normalize(long nodeId) {
        return (properties.doubleValue(nodeId) - min) / (max - min);
    }

    static MinMaxNormalizer create(NodeProperties properties, long nodeCount) {
        var max = Double.MIN_VALUE;
        var min = Double.MAX_VALUE;

        for (long nodeId = 0; nodeId < nodeCount; nodeId++) {
            var propertyValue = properties.doubleValue(nodeId);
            if (propertyValue < min) {
                min = propertyValue;
            }
            if (propertyValue > max) {
                max = propertyValue;
            }
        }

        return new MinMaxNormalizer(properties, min, max);
    }
}

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
package org.neo4j.gds.scaleproperties;

import org.neo4j.gds.AlgorithmMemoryEstimateDefinition;
import org.neo4j.gds.collections.ha.HugeObjectArray;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.mem.MemoryEstimations;
import org.neo4j.gds.core.utils.mem.MemoryRange;
import org.neo4j.gds.mem.MemoryUsage;

import java.util.List;

public class ScalePropertiesMemoryEstimateDefinition implements AlgorithmMemoryEstimateDefinition {

    private static final int ESTIMATED_DIMENSION_PER_PROPERTY = 128;

    private final List<String> nodeProperties;

    public ScalePropertiesMemoryEstimateDefinition(List<String> nodeProperties) {
        this.nodeProperties = nodeProperties;
    }

    @Override
    public MemoryEstimation memoryEstimation() {
        MemoryEstimations.Builder builder = MemoryEstimations.builder("Scale properties");

        builder.perGraphDimension("Scaled properties", (graphDimensions, concurrency) -> {
                int totalPropertyDimension = nodeProperties
                    .stream()
                    .mapToInt(p -> graphDimensions
                        .nodePropertyDimensions()
                        .get(p)
                        .orElse(ESTIMATED_DIMENSION_PER_PROPERTY))
                    .sum();

                return MemoryRange.of(HugeObjectArray.memoryEstimation(
                    graphDimensions.nodeCount(),
                    MemoryUsage.sizeOfDoubleArray(totalPropertyDimension)
                ));
            }
        );
        return builder.build();
    }

}

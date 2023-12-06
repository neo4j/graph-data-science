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
package org.neo4j.gds.leiden;

import org.neo4j.gds.AlgorithmMemoryEstimateDefinition;
import org.neo4j.gds.collections.ha.HugeDoubleArray;
import org.neo4j.gds.collections.ha.HugeLongArray;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.mem.MemoryEstimations;

public class LeidenMemoryEstimateDefinition implements AlgorithmMemoryEstimateDefinition<LeidenBaseConfig> {

    @Override
    public MemoryEstimation memoryEstimation(LeidenBaseConfig configuration) {
        var builder = MemoryEstimations.builder(Leiden.class)
            .perNode("local move communities", HugeLongArray::memoryEstimation)
            .perNode("local move node volumes", HugeDoubleArray::memoryEstimation)
            .perNode("local move community volumes", HugeDoubleArray::memoryEstimation)
            .perNode("current communities", HugeLongArray::memoryEstimation);
        if (configuration.seedProperty() != null) {
            builder.add("seeded communities", SeedCommunityManager.memoryEstimation());
        }
        builder
            .add("local move phase", LocalMovePhase.estimation())
            .add("modularity computation", ModularityComputer.estimation())
            .add("dendogram manager", LeidenDendrogramManager.memoryEstimation(
                configuration.includeIntermediateCommunities() ? configuration.maxLevels() : 1
            ))
            .add("refinement phase", RefinementPhase.memoryEstimation())
            .add("aggregation phase", GraphAggregationPhase.memoryEstimation())
            .add("post-aggregation phase", MemoryEstimations.builder()
                .perNode("next local move communities", HugeLongArray::memoryEstimation)
                .perNode("next local move node volumes", HugeDoubleArray::memoryEstimation)
                .perNode("next local move community volumes", HugeDoubleArray::memoryEstimation)
                .perNode("community to node map", HugeLongArray::memoryEstimation)
                .build()
            );
        return builder.build();
    }

}

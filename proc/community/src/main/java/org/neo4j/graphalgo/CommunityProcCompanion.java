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
package org.neo4j.graphalgo;

import org.neo4j.graphalgo.api.NodeProperties;
import org.neo4j.graphalgo.api.nodeproperties.ConsecutiveLongNodeProperties;
import org.neo4j.graphalgo.api.nodeproperties.LongIfChangedNodeProperties;
import org.neo4j.graphalgo.api.nodeproperties.LongNodeProperties;
import org.neo4j.graphalgo.config.AlgoBaseConfig;
import org.neo4j.graphalgo.config.ConsecutiveIdsConfig;
import org.neo4j.graphalgo.config.SeedConfig;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;

public class CommunityProcCompanion {

    public static <ALGO extends Algorithm<ALGO, RESULT>, RESULT, CONFIG extends AlgoBaseConfig & SeedConfig & ConsecutiveIdsConfig> NodeProperties nodeProperties(
        AlgoBaseProc.ComputationResult<ALGO, RESULT, CONFIG> computationResult,
        String resultProperty,
        LongNodeProperties nodeProperties,
        AllocationTracker tracker
    ) {
        var config = computationResult.config();
        var graphStore = computationResult.graphStore();

        var consecutiveIds = config.consecutiveIds();
        var isIncremental = config.isIncremental();
        var seedProperty = config.seedProperty();
        var resultPropertyEqualsSeedProperty = isIncremental && resultProperty.equals(seedProperty);

        if (resultPropertyEqualsSeedProperty && !consecutiveIds) {
            return LongIfChangedNodeProperties.of(graphStore, seedProperty, nodeProperties);
        } else if (consecutiveIds && !isIncremental) {
            return new ConsecutiveLongNodeProperties(
                nodeProperties,
                computationResult.graph().nodeCount(),
                tracker
            );
        } else {
            return nodeProperties;
        }
    }
}

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
package org.neo4j.gds.procedures.algorithms.pathfinding.stats;

import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.collections.ha.HugeLongArray;
import org.neo4j.gds.procedures.algorithms.results.StandardStatsResult;
import org.neo4j.gds.result.TimedAlgorithmResult;
import org.neo4j.gds.results.ResultTransformerBuilder;

import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Stream;

class TraversalStatsResultTransformerBuilder implements ResultTransformerBuilder<TimedAlgorithmResult<HugeLongArray>, Stream<StandardStatsResult>> {

    private final Supplier<Map<String, Object>> configurationSupplier;

    TraversalStatsResultTransformerBuilder(Supplier<Map<String, Object>> configurationSupplier) {
        this.configurationSupplier = configurationSupplier;
    }

    @Override
    public TraversalStatsResultTransformer build(
        Graph graph,
        GraphStore graphStore
    ) {
        return new TraversalStatsResultTransformer(configurationSupplier);
    }
}

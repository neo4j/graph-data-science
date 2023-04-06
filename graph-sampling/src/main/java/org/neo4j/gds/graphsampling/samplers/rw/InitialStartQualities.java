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
package org.neo4j.gds.graphsampling.samplers.rw;

import com.carrotsearch.hppc.DoubleArrayList;
import com.carrotsearch.hppc.DoubleCollection;
import com.carrotsearch.hppc.LongArrayList;
import com.carrotsearch.hppc.LongCollection;
import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.api.Graph;

import java.util.List;
import java.util.SplittableRandom;

@ValueClass
public
interface InitialStartQualities {
    LongCollection nodeIds();

    DoubleCollection qualities();

    static InitialStartQualities init(Graph inputGraph, SplittableRandom rng, List<Long> startNodes) {
        var nodeIds = new LongArrayList();
        var qualities = new DoubleArrayList();

        if (!startNodes.isEmpty()) {
            startNodes.forEach(nodeId -> {
                nodeIds.add(inputGraph.toMappedNodeId(nodeId));
                qualities.add(1.0);
            });
        } else {
            nodeIds.add(rng.nextLong(inputGraph.nodeCount()));
            qualities.add(1.0);
        }

        return ImmutableInitialStartQualities.of(nodeIds, qualities);
    }
}

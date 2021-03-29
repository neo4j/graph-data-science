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
package org.neo4j.graphalgo.beta.pregel;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.utils.paged.HugeAtomicBitSet;

abstract class PregelComputer<CONFIG extends PregelConfig> {
    final Graph graph;
    final PregelComputation<CONFIG> computation;
    final CONFIG config;
    final NodeValue nodeValues;
    final Messenger<?> messenger;
    final HugeAtomicBitSet voteBits;

    PregelComputer(
        Graph graph,
        PregelComputation<CONFIG> computation,
        CONFIG config,
        NodeValue nodeValues,
        Messenger<?> messenger,
        HugeAtomicBitSet voteBits
    ) {
        this.graph = graph;
        this.computation = computation;
        this.config = config;
        this.nodeValues = nodeValues;
        this.messenger = messenger;
        this.voteBits = voteBits;
    }

    abstract void initComputation();

    abstract void initIteration(int iteration);

    abstract void runIteration();

    abstract boolean hasConverged();
}

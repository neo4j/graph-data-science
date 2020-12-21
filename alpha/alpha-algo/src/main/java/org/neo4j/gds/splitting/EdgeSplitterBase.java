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
package org.neo4j.gds.splitting;

import org.neo4j.graphalgo.annotation.ValueClass;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.Relationships;

import java.util.Random;

public class EdgeSplitterBase {

    static final double NEGATIVE = 0D;
    static final double POSITIVE = 1D;

    protected final ThreadLocal<Random> rng;

    public EdgeSplitterBase(long seed) {
        this.rng = ThreadLocal.withInitial(() -> new Random(seed));
    }

    protected boolean sample(double probability) {
        return rng.get().nextDouble() < probability;
    }

    protected long randomNodeId(Graph graph) {
        return Math.abs(rng.get().nextLong() % graph.nodeCount());
    }

    protected long samplesPerNode(long maxSamples, double remainingSamples, long remainingNodes) {
        var numSamplesOnAverage = remainingSamples / remainingNodes;
        var wholeSamples = (long) numSamplesOnAverage;
        var extraSample = sample(numSamplesOnAverage - wholeSamples) ? 1 : 0;
        return Math.min(maxSamples, wholeSamples + extraSample);
    }

    @ValueClass
    interface SplitResult {
        Relationships remainingRels();
        Relationships selectedRels();

        static DirectedEdgeSplitter.SplitResult of(Relationships remainingRels, Relationships selectedRels) {
            return ImmutableSplitResult.of(remainingRels, selectedRels);
        }
    }
}

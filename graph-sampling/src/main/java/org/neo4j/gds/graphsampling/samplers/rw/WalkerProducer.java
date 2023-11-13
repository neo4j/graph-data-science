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


import org.neo4j.gds.api.Graph;
import org.neo4j.gds.collections.haa.HugeAtomicDoubleArray;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.graphsampling.config.RandomWalkWithRestartsConfig;
import org.neo4j.gds.graphsampling.samplers.SeenNodes;
import org.neo4j.gds.graphsampling.samplers.rw.cnarw.CNARWNodeSamplingStrategySupplier;
import org.neo4j.gds.graphsampling.samplers.rw.rwr.RWRNodeSamplingStrategySupplier;

import java.util.Optional;
import java.util.SplittableRandom;

public class WalkerProducer {

private final NodeSamplingStrategySupplier nodeSamplingStrategySupplier;

    private WalkerProducer(NodeSamplingStrategySupplier nodeSamplingStrategySupplier) {
        this.nodeSamplingStrategySupplier = nodeSamplingStrategySupplier;
    }

    public Runnable getWalker(
        SeenNodes seenNodes,
        Optional<HugeAtomicDoubleArray> totalWeights,
        double qualityThreshold,
        WalkQualities walkQualities,
        SplittableRandom split,
        Graph concurrentCopy,
        RandomWalkWithRestartsConfig config,
        ProgressTracker progressTracker
    ) {
        return new Walker(
            seenNodes,
            totalWeights,
            qualityThreshold,
            walkQualities,
            split,
            concurrentCopy,
            config.restartProbability(),
            progressTracker,
            nodeSamplingStrategySupplier.apply(concurrentCopy, split, totalWeights)
        );
    }

   public static  WalkerProducer CNARWWalkerProducer(){
        return  new WalkerProducer(new CNARWNodeSamplingStrategySupplier());
    }

    public static  WalkerProducer RWRWalkerProducer(){
        return  new WalkerProducer(new RWRNodeSamplingStrategySupplier());
    }
}

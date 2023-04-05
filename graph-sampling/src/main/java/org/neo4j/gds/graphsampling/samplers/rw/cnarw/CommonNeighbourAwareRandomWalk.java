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
package org.neo4j.gds.graphsampling.samplers.rw.cnarw;

import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.core.utils.paged.HugeAtomicDoubleArray;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.gds.core.utils.progress.tasks.Tasks;
import org.neo4j.gds.graphsampling.config.CommonNeighbourAwareRandomWalkConfig;
import org.neo4j.gds.graphsampling.config.RandomWalkWithRestartsConfig;
import org.neo4j.gds.graphsampling.samplers.SeenNodes;
import org.neo4j.gds.graphsampling.samplers.rw.WalkQualities;
import org.neo4j.gds.graphsampling.samplers.rw.rwr.RandomWalkWithRestarts;
import org.neo4j.gds.graphsampling.samplers.rw.Walker;

import java.util.Optional;
import java.util.SplittableRandom;

public class CommonNeighbourAwareRandomWalk extends RandomWalkWithRestarts {

    private final CommonNeighbourAwareRandomWalkConfig config;

    public CommonNeighbourAwareRandomWalk(CommonNeighbourAwareRandomWalkConfig config) {
        super(config);
        this.config = config;
    }

    @Override
    public Task progressTask(GraphStore graphStore) {
        if (config.nodeLabelStratification()) {
            return Tasks.task(
                "Sample nodes",
                Tasks.leaf("Count node labels", graphStore.nodeCount()),
                Tasks.leaf(getSubTaskMessage(), 10 * Math.round(graphStore.nodeCount() * config.samplingRatio()))
            );
        } else {
            return Tasks.task(
                "Sample nodes",
                Tasks.leaf(getSubTaskMessage(), 10 * Math.round(graphStore.nodeCount() * config.samplingRatio()))
            );
        }
    }

    @Override
    public String progressTaskName() {
        return "Common neighbour aware random walks sampling";
    }

    @Override
    protected String getSubTaskMessage() {return "Do common neighbour aware random walks";}

    @Override
    public Runnable getWalker(
        SeenNodes seenNodes,
        Optional<HugeAtomicDoubleArray> totalWeights,
        double v,
        WalkQualities walkQualities,
        SplittableRandom split,
        Graph concurrentCopy,
        RandomWalkWithRestartsConfig config,
        ProgressTracker progressTracker
    ) {
        return new Walker(seenNodes, totalWeights, v, walkQualities, split, concurrentCopy, config, progressTracker,
            new CommonNeighbourAwareNextNodeStrategy(concurrentCopy, totalWeights.isPresent(), split)
        );
    }
}

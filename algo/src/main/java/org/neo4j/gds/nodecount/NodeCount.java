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
package org.neo4j.gds.nodecount;

import org.neo4j.gds.Algorithm;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.termination.TerminationFlag;

/**
 * A deliberately trivial example algorithm: it counts the number of nodes in the graph and returns it.
 * <p>
 * Its only purpose is to act as a template that shows the full set of modules and wiring required to add a
 * new algorithm to GDS, without the distraction of any real algorithmic complexity. To implement a real
 * algorithm, replace the body of {@link #compute()} with the actual computation; everything around it
 * (config, parameters, facades, procedures, progress tracking) follows the same shape demonstrated here.
 */
public class NodeCount extends Algorithm<NodeCountResult> {

    private final Graph graph;

    public NodeCount(Graph graph, ProgressTracker progressTracker, TerminationFlag terminationFlag) {
        super(progressTracker);
        this.graph = graph;
        this.terminationFlag = terminationFlag;
    }

    @Override
    public NodeCountResult compute() {
        progressTracker.beginSubTask();

        terminationFlag.assertRunning();
        var nodeCount = graph.nodeCount();
        progressTracker.onProgress(nodeCount);

        progressTracker.endSubTask();

        return new NodeCountResult(nodeCount);
    }
}

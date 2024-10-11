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
package org.neo4j.gds.graphsampling;

import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.core.utils.paged.HugeAtomicBitSet;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.gds.termination.TerminationFlag;

public abstract class NodesSampler {
    protected abstract HugeAtomicBitSet compute(
        Graph inputGraph,
        ProgressTracker progressTracker
    );

    protected abstract Task progressTask(GraphStore graphStore);

    protected abstract String progressTaskName();

    protected volatile TerminationFlag terminationFlag = TerminationFlag.RUNNING_TRUE;

    public void setTerminationFlag(TerminationFlag terminationFlag) {
        this.terminationFlag = terminationFlag;
    }

    public TerminationFlag getTerminationFlag() {
        return terminationFlag;
    }
}

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
package org.neo4j.gds.wcc;

import org.neo4j.gds.api.Graph;
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmMachinery;
import org.neo4j.gds.core.concurrency.DefaultPool;
import org.neo4j.gds.core.concurrency.ParallelUtil;
import org.neo4j.gds.core.utils.paged.dss.DisjointSetStruct;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.termination.TerminationFlag;

/**
 * WCC is used by other algorithms. So we encapsulate it here.
 */
public class WccStub {
    private final TerminationFlag terminationFlag;

    private final AlgorithmMachinery algorithmMachinery;

    public WccStub(TerminationFlag terminationFlag, AlgorithmMachinery algorithmMachinery) {
        this.terminationFlag = terminationFlag;
        this.algorithmMachinery = algorithmMachinery;
    }

    public DisjointSetStruct wcc(Graph graph, WccParameters parameters, ProgressTracker progressTracker) {
        var algorithm = new Wcc(
            graph,
            DefaultPool.INSTANCE,
            ParallelUtil.DEFAULT_BATCH_SIZE,
            parameters,
            progressTracker,
            terminationFlag
        );

        return algorithmMachinery.runAlgorithmsAndManageProgressTracker(algorithm, progressTracker, true, parameters.concurrency());
    }
}

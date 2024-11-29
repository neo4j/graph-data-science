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
package org.neo4j.gds.modularityoptimization;

import org.neo4j.gds.api.Graph;
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmMachinery;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.concurrency.DefaultPool;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.k1coloring.K1Coloring;
import org.neo4j.gds.k1coloring.K1ColoringParameters;
import org.neo4j.gds.k1coloring.K1ColoringResult;
import org.neo4j.gds.termination.TerminationFlag;

public class K1ColoringStub {
    private final AlgorithmMachinery algorithmMachinery;

    public K1ColoringStub(AlgorithmMachinery algorithmMachinery) {
        this.algorithmMachinery = algorithmMachinery;
    }

    public K1ColoringResult k1Coloring(
        Graph graph,
        K1ColoringParameters parameters,
        ProgressTracker progressTracker,
        TerminationFlag terminationFlag,
        Concurrency concurrency,
        boolean shouldReleaseProgressTracker
    ) {
        var algorithm = new K1Coloring(
            graph,
            parameters.maxIterations(),
            parameters.batchSize(),
            parameters.concurrency(),
            DefaultPool.INSTANCE,
            progressTracker,
            terminationFlag
        );

        return algorithmMachinery.runAlgorithmsAndManageProgressTracker(
            algorithm,
            progressTracker,
            shouldReleaseProgressTracker,
            concurrency
        );
    }
}

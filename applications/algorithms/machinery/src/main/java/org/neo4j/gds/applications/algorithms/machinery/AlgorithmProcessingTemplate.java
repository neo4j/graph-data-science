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
package org.neo4j.gds.applications.algorithms.machinery;

import org.neo4j.gds.api.GraphName;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;

import java.util.Optional;
import java.util.function.Supplier;

/**
 * So, internally, all (pathfinding) algorithms in all modes follow these same steps:
 * <ol>
 *     <li>You load a graph
 *     <li>You validate the algorithms vs the graph (is it empty, memory usage, etc.)
 *     <li>You establish timings and metrics
 *     <li>YOU CALL THE ACTUAL ALGORITHM! Minor detail...
 *     <li>Next up, mutates or writes, the possible side effect steps
 *     <li>Lastly result rendering
 * </ol>
 *  Result rendering is a whole chapter. It is independent of where call came from, so injected.
 *  It can use any of these parameters:
 *  <ul>
 *      <li>graph
 *      <li>graph store
 *      <li>configuration
 *      <li>timings, including side effect steps
 *  </ul>
 *  Injected, plus mode-conditional: it's a wide parameter list...
 *  There is always a result. For streaming, it is duh; for the others it is metadata.
 *  Side effect steps are optional, and they are injected in some sense, but also _selected_.
 *  Because luckily those behaviours are generic enough that you can just select them from a catalogue.
 *  So we _instrument_ with zero or one mode behaviour selected from a catalogue,
 *  and _instrument_ with bespoke result rendering.
 */
public interface AlgorithmProcessingTemplate {
    <CONFIGURATION extends AlgoBaseConfig, RESULT_TO_CALLER, RESULT_FROM_ALGORITHM>
    RESULT_TO_CALLER processAlgorithm(
        GraphName graphName,
        CONFIGURATION configuration,
        String humanReadableAlgorithmName,
        Supplier<MemoryEstimation> estimationFactory,
        AlgorithmComputation<RESULT_FROM_ALGORITHM> algorithmComputation,
        Optional<MutateOrWriteStep<RESULT_FROM_ALGORITHM>> mutateOrWriteStep,
        ResultBuilder<CONFIGURATION, RESULT_FROM_ALGORITHM, RESULT_TO_CALLER> resultBuilder
    );
}

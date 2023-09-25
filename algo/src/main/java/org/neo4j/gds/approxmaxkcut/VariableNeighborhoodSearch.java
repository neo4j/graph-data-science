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
package org.neo4j.gds.approxmaxkcut;

import org.neo4j.gds.api.Graph;
import org.neo4j.gds.approxmaxkcut.config.ApproxMaxKCutBaseConfig;
import org.neo4j.gds.approxmaxkcut.localsearch.LocalSearch;
import org.neo4j.gds.collections.ha.HugeByteArray;
import org.neo4j.gds.core.concurrency.AtomicDouble;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;

import java.util.SplittableRandom;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.function.BooleanSupplier;

class VariableNeighborhoodSearch {

    private final Graph graph;
    private final SplittableRandom random;
    private final ApproxMaxKCut.Comparator comparator;
    private final ApproxMaxKCutBaseConfig config;
    private final LocalSearch localSearch;
    private final HugeByteArray[] candidateSolutions;
    private final AtomicDouble[] costs;
    private final ProgressTracker progressTracker;
    private HugeByteArray neighborSolution;
    private AtomicLongArray neighborCardinalities;

    VariableNeighborhoodSearch(
        Graph graph,
        SplittableRandom random,
        ApproxMaxKCut.Comparator comparator,
        ApproxMaxKCutBaseConfig config,
        LocalSearch localSearch,
        HugeByteArray[] candidateSolutions,
        AtomicDouble[] costs,
        ProgressTracker progressTracker
    ) {
        this.graph = graph;
        this.random = random;
        this.comparator = comparator;
        this.config = config;
        this.localSearch = localSearch;
        this.candidateSolutions = candidateSolutions;
        this.costs = costs;
        this.progressTracker = progressTracker;

        this.neighborSolution = HugeByteArray.newArray(graph.nodeCount());
        this.neighborCardinalities = new AtomicLongArray(config.k());
    }

    AtomicLongArray compute(int candidateIdx, AtomicLongArray currentCardinalities, BooleanSupplier running) {
        var bestCandidateSolution = candidateSolutions[candidateIdx];
        var bestCardinalities = currentCardinalities;
        var bestCost = costs[candidateIdx];
        var neighborCost = new AtomicDouble();
        var currentOrder = 0;

        progressTracker.beginSubTask();

        while ((currentOrder < config.vnsMaxNeighborhoodOrder()) && running.getAsBoolean()) {
            boolean perturbSuccess = true;
            bestCandidateSolution.copyTo(neighborSolution, graph.nodeCount());
            copyCardinalities(bestCardinalities, neighborCardinalities);

            // Generate a neighboring candidate solution of the current currentOrder.
            int order = 0;
            for (; order < currentOrder; order++) {
                perturbSuccess = perturbSolution(neighborSolution, neighborCardinalities);
                if (!perturbSuccess) {
                    break;
                }
            }

            if (currentOrder > 0 && order == 0) {
                // We were not able to perturb at all so no point in even trying local search again.
                break;
            }

            localSearch.compute(neighborSolution, neighborCost, neighborCardinalities, running);

            if (comparator.compare(neighborCost.get(), bestCost.get())) {
                var tmpCandidateSolution = bestCandidateSolution;
                bestCandidateSolution = neighborSolution;
                neighborSolution = tmpCandidateSolution;

                var tmpCardinalities = bestCardinalities;
                bestCardinalities = neighborCardinalities;
                neighborCardinalities = tmpCardinalities;

                bestCost.set(neighborCost.get());

                // Start from scratch with the new candidate.
                currentOrder = 0;
            } else {
                if (!perturbSuccess) {
                    // We were not able to perturb this solution further, so let's stop.
                    break;
                }

                currentOrder += 1;
            }
        }

        // If we obtained a better candidate solution from VNS, swap with that with the one we started with.
        if (bestCandidateSolution != candidateSolutions[candidateIdx]) {
            neighborSolution = candidateSolutions[candidateIdx];
            candidateSolutions[candidateIdx] = bestCandidateSolution;
        }

        progressTracker.endSubTask();

        return bestCardinalities;
    }

    private boolean perturbSolution(
        HugeByteArray solution,
        AtomicLongArray cardinalities
    ) {
        final int MAX_RETRIES = 100;
        int retries = 0;

        while (retries < MAX_RETRIES) {
            long nodeToFlip = random.nextLong(0, graph.nodeCount());
            byte currentCommunity = solution.get(nodeToFlip);

            if (cardinalities.get(currentCommunity) <= config.minCommunitySizes().get(currentCommunity)) {
                // Flipping this node would invalidate the solution in terms of min community sizes.
                retries++;
                continue;
            }

            // For `nodeToFlip`, move to a new random community not equal to its current community in
            // `neighboringSolution`.
            byte rndNewCommunity = (byte) ((solution.get(nodeToFlip) + (random.nextInt(config.k() - 1) + 1))
                                           % config.k());

            solution.set(nodeToFlip, rndNewCommunity);
            cardinalities.decrementAndGet(currentCommunity);
            cardinalities.incrementAndGet(rndNewCommunity);

            break;
        }

        return retries != MAX_RETRIES;
    }

    private static void copyCardinalities(AtomicLongArray source, AtomicLongArray target) {
        assert target.length() >= source.length();

        for (int i = 0; i < source.length(); i++) {
            target.setPlain(i, source.getPlain(i));
        }
    }
}

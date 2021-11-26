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
package org.neo4j.gds.impl.approxmaxkcut;

import org.neo4j.gds.Algorithm;
import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.nodeproperties.LongNodeProperties;
import org.neo4j.gds.core.utils.AtomicDoubleArray;
import org.neo4j.gds.core.utils.mem.AllocationTracker;
import org.neo4j.gds.core.utils.paged.HugeByteArray;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;

import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLongArray;

import static org.neo4j.gds.impl.approxmaxkcut.PlaceNodesRandomly.randomNonNegativeLong;

/*
 * Implements a parallelized version of a GRASP (optionally with VNS) maximum k-cut approximation algorithm.
 *
 * A serial version of the algorithm with a slightly different construction phase is outlined in [1] as GRASP(+VNS) for
 * k = 2, and is known as FES02G(V) in [2] which benchmarks it against a lot of other algorithms, also for k = 2.
 *
 * TODO: Add the path-relinking heuristic for possibly slightly better results when running single-threaded (basically
 *  making the algorithm GRASP+VNS+PR in [1] and FES02GVP in [2]).
 *
 * [1]: Festa et al. Randomized Heuristics for the Max-Cut Problem, 2002.
 * [2]: Dunning et al. What Works Best When? A Systematic Evaluation of Heuristics for Max-Cut and QUBO, 2018.
 */
public class ApproxMaxKCut extends Algorithm<ApproxMaxKCut, ApproxMaxKCut.CutResult> {

    private Graph graph;
    private final Random random;
    private final ApproxMaxKCutConfig config;
    private final AllocationTracker allocationTracker;
    private final Comparator comparator;
    private final PlaceNodesRandomly placeNodesRandomly;
    private final LocalSearch localSearch;
    private final HugeByteArray[] candidateSolutions;
    private final AtomicDoubleArray[] costs;
    private AtomicLongArray currCardinalities;
    private HugeByteArray neighborSolution;
    private AtomicLongArray neighborCardinalities;

    public ApproxMaxKCut(
        Graph graph,
        ExecutorService executor,
        ApproxMaxKCutConfig config,
        ProgressTracker progressTracker,
        AllocationTracker allocationTracker
    ) {
        super(progressTracker);
        this.graph = graph;
        this.random = new Random(config.randomSeed().orElseGet(() -> new Random().nextLong()));
        this.config = config;
        this.allocationTracker = allocationTracker;
        this.comparator = config.minimize() ? (lhs, rhs) -> lhs < rhs : (lhs, rhs) -> lhs > rhs;

        // We allocate two arrays in order to be able to compare results between iterations "GRASP style".
        this.candidateSolutions = new HugeByteArray[]{
            HugeByteArray.newArray(graph.nodeCount(), allocationTracker),
            HugeByteArray.newArray(graph.nodeCount(), allocationTracker)
        };

        this.costs = new AtomicDoubleArray[]{
            new AtomicDoubleArray(1),
            new AtomicDoubleArray(1)
        };
        costs[0].set(0, config.minimize() ? Double.MAX_VALUE : Double.MIN_VALUE);
        costs[1].set(0, config.minimize() ? Double.MAX_VALUE : Double.MIN_VALUE);

        this.currCardinalities = new AtomicLongArray(config.k());

        this.placeNodesRandomly = new PlaceNodesRandomly(
            config,
            random,
            graph,
            executor,
            progressTracker
        );
        this.localSearch = new LocalSearch(
            graph,
            comparator,
            config,
            executor,
            progressTracker,
            allocationTracker
        );
    }

    @ValueClass
    public interface CutResult {
        // Value at index `i` is the idx of the community to which node with id `i` belongs.
        HugeByteArray candidateSolution();

        double cutCost();

        static CutResult of(
            HugeByteArray candidateSolution,
            double cutCost
        ) {
            return ImmutableCutResult
                .builder()
                .candidateSolution(candidateSolution)
                .cutCost(cutCost)
                .build();
        }

        default LongNodeProperties asNodeProperties() {
            return candidateSolution().asNodeProperties();
        }
    }

    @FunctionalInterface
    interface Comparator {
        boolean accept(double lhs, double rhs);
    }

    @Override
    public CutResult compute() {
        // Keep track of which candidate solution is currently being used and which is best.
        byte currIdx = 0, bestIdx = 1;

        progressTracker.beginSubTask();

        if (config.vnsMaxNeighborhoodOrder() > 0) {
            neighborSolution = HugeByteArray.newArray(graph.nodeCount(), allocationTracker);
            neighborCardinalities = new AtomicLongArray(config.k());
        }

        for (int i = 1; (i <= config.iterations()) && running(); i++) {
            var currCandidateSolution = candidateSolutions[currIdx];
            var currCost = costs[currIdx];

            placeNodesRandomly.compute(currCandidateSolution, currCardinalities);

            if (!running()) break;

            if (config.vnsMaxNeighborhoodOrder() > 0) {
                variableNeighborhoodSearch(currIdx);
            } else {
                localSearch.compute(currCandidateSolution, currCost, currCardinalities, this::running);
            }

            // Store the newly computed candidate solution if it was better than the previous. Then reuse the previous data
            // structures to make a new solution candidate if we are doing more iterations.
            if (comparator.accept(currCost.get(0), costs[bestIdx].get(0))) {
                var tmp = bestIdx;
                bestIdx = currIdx;
                currIdx = tmp;
            }
        }

        progressTracker.endSubTask();

        return CutResult.of(candidateSolutions[bestIdx], costs[bestIdx].get(0));
    }

    private boolean perturbSolution(
        HugeByteArray solution,
        AtomicLongArray cardinalities
    ) {
        final int MAX_RETRIES = 100;
        int retries = 0;

        while (retries < MAX_RETRIES) {
            long nodeToFlip = randomNonNegativeLong(random, 0, graph.nodeCount());
            byte currCommunity = solution.get(nodeToFlip);

            if (cardinalities.get(currCommunity) <= config.minCommunitySizes().get(currCommunity)) {
                // Flipping this node will invalidate the solution in terms on min community sizes.
                retries++;
                continue;
            }

            // For `nodeToFlip`, move to a new random community not equal to its current community in
            // `neighboringSolution`.
            byte rndNewCommunity = (byte) ((solution.get(nodeToFlip) + (random.nextInt(config.k() - 1) + 1))
                                           % config.k());

            solution.set(nodeToFlip, rndNewCommunity);
            cardinalities.decrementAndGet(currCommunity);
            cardinalities.incrementAndGet(rndNewCommunity);

            break;
        }

        return retries != MAX_RETRIES;
    }

    private void copyCardinalities(AtomicLongArray source, AtomicLongArray target) {
        assert target.length() >= source.length();

        for (int i = 0; i < source.length(); i++) {
            target.setPlain(i, source.getPlain(i));
        }
    }

    private void variableNeighborhoodSearch(int candidateIdx) {
        var bestCandidateSolution = candidateSolutions[candidateIdx];
        var bestCardinalities = currCardinalities;
        var bestCost = costs[candidateIdx];
        var neighborCost = new AtomicDoubleArray(1);
        boolean perturbSuccess = true;
        var order = 0;

        progressTracker.beginSubTask();

        while ((order < config.vnsMaxNeighborhoodOrder()) && running()) {
            bestCandidateSolution.copyTo(neighborSolution, graph.nodeCount());
            copyCardinalities(bestCardinalities, neighborCardinalities);

            // Generate a neighboring candidate solution of the current order.
            for (int i = 0; i < order; i++) {
                perturbSuccess = perturbSolution(neighborSolution, neighborCardinalities);
                if (!perturbSuccess) {
                    break;
                }
            }

            localSearch.compute(neighborSolution, neighborCost, neighborCardinalities, this::running);

            if (comparator.accept(neighborCost.get(0), bestCost.get(0))) {
                var tmpCandidateSolution = bestCandidateSolution;
                bestCandidateSolution = neighborSolution;
                neighborSolution = tmpCandidateSolution;

                var tmpCardinalities = bestCardinalities;
                bestCardinalities = neighborCardinalities;
                neighborCardinalities = tmpCardinalities;

                bestCost.set(0, neighborCost.get(0));

                // Start from scratch with the new candidate.
                order = 0;
            } else {
                order += 1;
            }

            if (!perturbSuccess) {
                // We were not able to perturb this solution further, so let's stop.
                break;
            }
        }

        // If we obtained a better candidate solution from VNS, swap with that with the one we started with.
        if (bestCandidateSolution != candidateSolutions[candidateIdx]) {
            neighborSolution = candidateSolutions[candidateIdx];
            candidateSolutions[candidateIdx] = bestCandidateSolution;
            currCardinalities = bestCardinalities;
        }

        progressTracker.endSubTask();
    }

    @Override
    public ApproxMaxKCut me() {
        return this;
    }

    @Override
    public void release() {
        graph = null;
    }
}

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
package org.neo4j.graphalgo.impl.ocd;

import org.neo4j.graphalgo.Algorithm;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.logging.Log;

import java.util.concurrent.ExecutorService;

public class OverlappingCommunityDetection extends Algorithm<OverlappingCommunityDetection, CommunityAffiliations> {
    private static final double TOLERANCE = 0.00001;

    private final Graph graph;
    private final AffiliationInitializer initializer;
    private final KernelTransaction transaction;
    private ExecutorService executorService;
    private final AllocationTracker tracker;
    private final Log log;

    OverlappingCommunityDetection(
        Graph graph,
        AffiliationInitializer initializer,
        KernelTransaction transaction,
        ExecutorService executorService,
        AllocationTracker tracker,
        Log log
    ) {
        this.graph = graph;
        this.initializer = initializer;
        this.transaction = transaction;
        this.executorService = executorService;
        this.tracker = tracker;
        this.log = log;
        if (graph.nodeCount() > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Overlapping community detection only supports graphs with 2^32-1 nodes.");
        }
    }

    @Override
    public CommunityAffiliations compute() {
        CommunityAffiliations communityAffiliations = initializer.initialize(graph);
        double oldGain = communityAffiliations.gain();
        double newGain;
        while ((newGain = update(communityAffiliations)) > oldGain + TOLERANCE) {
            oldGain = newGain;
        }
        return communityAffiliations;
    }


    double update(CommunityAffiliations communityAffiliations) {
        BacktrackingLineSearch lineSearch = new BacktrackingLineSearch();
        for (int nodeU = 0; nodeU < communityAffiliations.nodeCount(); nodeU++) {
            GainFunction blockLoss = communityAffiliations.blockGain(nodeU);
            SparseVector gradient = blockLoss.gradient();
            double learningRate = lineSearch.search(
                blockLoss,
                communityAffiliations.nodeAffiliations(nodeU),
                gradient
            );
            communityAffiliations.updateNodeAffiliations(nodeU, gradient.multiply(learningRate));
        }
        return communityAffiliations.gain();
    }

    @Override
    public OverlappingCommunityDetection me() {
        return this;
    }

    @Override
    public void release() {

    }
}

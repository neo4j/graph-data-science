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
package org.neo4j.gds.pricesteiner;

import com.carrotsearch.hppc.BitSet;
import org.agrona.collections.MutableLong;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.collections.ha.HugeDoubleArray;
import org.neo4j.gds.collections.ha.HugeLongArray;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.termination.TerminationFlag;

import java.util.function.LongToDoubleFunction;

class GrowthPhase {
    private final ClusterStructure clusterStructure;
    private final Graph graph;
    private final ClusterEventsPriorityQueue clusterEventsPriorityQueue;
    private final HugeLongArray edgeParts;
    private final HugeDoubleArray edgeCosts;
    private final EdgeEventsQueue edgeEventsQueue;
    private final LongToDoubleFunction prizes;
    private final HugeLongArray treeEdges;
    private final ProgressTracker progressTracker;
    private final TerminationFlag terminationFlag;
    private long numberOfTreeEdges;

    private final double EPS = 1E-6;

    GrowthPhase(Graph graph, LongToDoubleFunction prizes, ProgressTracker progressTracker, TerminationFlag terminationFlag) {
        //TODO: INITIALIZE some of these data/structures with n memory instead of 2*n
        this.graph = graph;
        this.clusterStructure = new ClusterStructure(graph.nodeCount());
        this.clusterEventsPriorityQueue = new ClusterEventsPriorityQueue(graph.nodeCount());
        this.edgeParts = HugeLongArray.newArray(graph.relationshipCount());
        this.edgeCosts = HugeDoubleArray.newArray(graph.relationshipCount() / 2);
        this.edgeEventsQueue = new EdgeEventsQueue(graph.nodeCount());
        this.prizes = prizes;
        this.treeEdges = HugeLongArray.newArray(graph.nodeCount());
        this.progressTracker = progressTracker;
        this.terminationFlag = terminationFlag;
        numberOfTreeEdges = 0;
    }

    GrowthResult grow() {
        //initialization
        progressTracker.beginSubTask("Growth Phase");

        progressTracker.beginSubTask("Initialization");
        initializeClusterPrizes();
        initializeEdgeParts();
        progressTracker.endSubTask("Initialization");

        progressTracker.beginSubTask("Growing");
        double moat;
        while (clusterStructure.numberOfActiveClusters() > 1) {
            terminationFlag.assertRunning();
            double edgeEventTime = edgeEventsQueue.nextEventTime();
            double clusterEventTime = clusterEventsPriorityQueue.closestEvent(clusterStructure.active());
            if (Double.compare(clusterEventTime, edgeEventTime) <= 0) {
                moat = clusterEventTime;
                deactivateCluster(moat, clusterEventsPriorityQueue.topCluster());
                progressTracker.logProgress();
            } else {
                moat = edgeEventTime;

                long uPart = edgeEventsQueue.topEdgePart();
                long vPart = otherEdgePart(uPart);
                long u = edgeParts.get(uPart);
                long v = edgeParts.get(vPart);

                edgeEventsQueue.pop();

                if (u<0 || v<0) {
                    continue;
                }

                ClusterMoatPair cmu = clusterStructure.sumOnEdgePart(u,moat);
                ClusterMoatPair cmv = clusterStructure.sumOnEdgePart(v,moat);

                var uCluster = cmu.cluster();
                var uClusterSum = cmu.totalMoat();

                var vCluster = cmv.cluster();
                var vClusterSum = cmv.totalMoat();


                if (vCluster == uCluster) {
                    edgeParts.set(uPart,-u);
                    edgeParts.set(vPart,-v);
                    continue;
                }

                long edgeId = partToEdgeId(uPart);
                double r = edgeCosts.get(edgeId) - uClusterSum - vClusterSum;
                if (Double.compare(r, 0) <= 0 || Double.compare(r, EPS) <= 0) {
                    mergeClusters(moat, uCluster, vCluster, edgeId);
                    progressTracker.logProgress();
                } else {
                    generateNewEdgeEvents(
                        moat,
                        uPart,
                        vPart,
                        uCluster,
                        vCluster,
                        r,
                        clusterStructure.active(vCluster)
                    );
                }

            }

        }
        progressTracker.endSubTask("Growing");
       progressTracker.endSubTask("Growth Phase");
        return new GrowthResult(
            treeEdges,
            numberOfTreeEdges,
            edgeParts,
            edgeCosts,
            clusterStructure.activeOriginalNodesOfCluster(clusterStructure.singleActiveCluster())
        );
    }

    private void deactivateCluster(double moat, long clusterId) {
        clusterEventsPriorityQueue.pop();
        clusterStructure.deactivateCluster(clusterId, moat);
        edgeEventsQueue.deactivateCluster(clusterId);
    }

    private void initializeEdgeParts() {
        MutableLong counter = new MutableLong();

        long nodeCount = graph.nodeCount();

        for (long u = 0; u < nodeCount; ++u) {
            graph.forEachRelationship(u, 1.0, (s, t, w) -> {

                if (s > t) {
                    var edgeId = counter.getAndIncrement();
                    var edgePart1 = 2 * edgeId;
                    var edgePart2 = 2 * edgeId + 1;
                    edgeCosts.set(edgeId, w);
                    edgeParts.set(2 * edgeId, s);
                    edgeParts.set(2 * edgeId + 1, t);
                    edgeEventsQueue.addBothWays(s, t, edgePart1, edgePart2, w / 2);
                }
                return  s > t;
            });
            progressTracker.logProgress(graph.degree(u));
        }
        edgeEventsQueue.performInitialAssignment(nodeCount);
    }

    private void initializeClusterPrizes() {
        for (long u = 0; u < graph.nodeCount(); ++u) {
            double prize = prizes.applyAsDouble(u);
            clusterStructure.setClusterPrize(u, prize);
            clusterEventsPriorityQueue.add(u, clusterStructure.tightnessTime(u, 0));
        }
    }

    private void mergeClusters(
        double moat,
        long cluster1,
        long cluster2,
        long edgeId
    ) {
        boolean cluster1Inactive = !clusterStructure.active(cluster1);
        boolean cluster2Inactive = !clusterStructure.active(cluster2);

        if (cluster1Inactive) {
            edgeEventsQueue.increaseValuesOnInactiveCluster(cluster1, moat - clusterStructure.inactiveSince(cluster1));
        }
        if (cluster2Inactive) {
            edgeEventsQueue.increaseValuesOnInactiveCluster(cluster2, moat - clusterStructure.inactiveSince(cluster2));
        }

        var newCluster = clusterStructure.merge(cluster1, cluster2,moat);

        edgeEventsQueue.mergeAndUpdate(newCluster, cluster1, cluster2);
        clusterEventsPriorityQueue.add(newCluster, clusterStructure.tightnessTime(newCluster, moat));

        addToTree(edgeId);
        edgeParts.set(2*edgeId,-edgeParts.get(2*edgeId));
        edgeParts.set(2*edgeId+1,-edgeParts.get(2*edgeId+1));
    }

    private void generateNewEdgeEvents(
        double moat,
        long uPart,
        long vPart,
        long uCluster,
        long vCluster,
        double r,
        boolean vClusterActive
    ) {
        if (vClusterActive) {
            edgeEventsQueue.addWithCheck(uCluster, uPart, moat + r / 2);
            edgeEventsQueue.addWithCheck(vCluster, vPart, moat + r / 2);
        } else {
            edgeEventsQueue.addWithCheck(uCluster, uPart, moat + r);
            /*
            * remember that we do a increase operation when merging queues, so the `moat` will become equal to mergeTimeStamp
            * and processed next iteration
            */
            edgeEventsQueue.addWithoutCheck(vCluster, vPart, moat);
        }
    }

    private long otherEdgePart(long i) {
        return (i + 1) - 2 * (i % 2);
    }

    private long partToEdgeId(long part) {
        return part / 2;
    }

    private void addToTree(long edgeId) {
        treeEdges.set(numberOfTreeEdges++, edgeId);
    }

    ClusterStructure clusterStructure(){
        return clusterStructure;
    }
}

record GrowthResult(HugeLongArray treeEdges,
                    long numberOfTreeEdges,
                    HugeLongArray edgeParts,
                    HugeDoubleArray edgeCosts,
                    BitSet activeOriginalNodes) {}

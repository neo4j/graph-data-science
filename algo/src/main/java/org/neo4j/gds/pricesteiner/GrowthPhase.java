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

import org.agrona.collections.MutableLong;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.collections.ha.HugeDoubleArray;
import org.neo4j.gds.collections.ha.HugeLongArray;

import java.util.function.LongToDoubleFunction;

class GrowthPhase {
    private final ClusterStructure clusterStructure;
    private final Graph graph;
    private final ClusterEventsPriorityQueue clusterEventsPriorityQueue;
    private final ClusterActivity clusterActivity;
    private final HugeLongArray edgeParts;
    private final HugeDoubleArray edgeCosts;
    private final EdgeEventsQueue edgeEventsQueue;
    private final LongToDoubleFunction prizes;

    private final double EPS = 1E-6;


    GrowthPhase(Graph graph, LongToDoubleFunction prizes) {
        //TODO: INITIALIZE some of these data/structures with n memory instead of 2*n
        this.graph = graph;
        this.clusterStructure = new ClusterStructure(graph.nodeCount());
        this.clusterEventsPriorityQueue = new ClusterEventsPriorityQueue(graph.nodeCount());
        this.clusterActivity = new ClusterActivity(graph.nodeCount());
        this.edgeParts = HugeLongArray.newArray(graph.relationshipCount());
        this.edgeCosts = HugeDoubleArray.newArray(graph.relationshipCount() / 2);
        this.edgeEventsQueue = new EdgeEventsQueue(graph.nodeCount());
        this.prizes = prizes;
    }

    void grow() {
        //initialization
        initializeClusterPrizes();
        initializeEdgeParts();
        double moat = 0;
        while (clusterActivity.numberOfActiveClusters() > 1) {
            double edgeEventTime = edgeEventsQueue.nextEventTime();
            double clusterEventTime = clusterEventsPriorityQueue.closestEvent(clusterActivity.active());
            if (Double.compare(clusterEventTime, edgeEventTime) <= 0) {
                moat = clusterEventTime;
                deactivateCluster(moat, clusterEventsPriorityQueue.topCluster());

            } else {
                //REMOVE
                moat = edgeEventTime;
                long uCluster = edgeEventsQueue.top();
                long uPart = edgeEventsQueue.topEdgePart();
                long vPart = otherEdgePart(uPart);
                long u = edgeParts.get(uPart);
                long v = edgeParts.get(vPart); //TODO: If we have already  processed other edge part (tight or delete), find shortcut to skip

                var uClusterSum = clusterStructure.sumOnEdgePartOnly(u);

                ClusterMoatPair cmv = clusterStructure.sumOnEdgePart(v);
                var vCluster = cmv.cluster();
                var vClusterSum = cmv.totalMoat();

                edgeEventsQueue.pop();

                if (vCluster == uCluster) {
                    continue;
                }
                double r = edgeCost(edgeCosts, uPart) - uClusterSum - vClusterSum;
                if (Double.compare(r, 0) <= 0 || Double.compare(r, EPS) <= 0) {
                    mergeClusters(moat, uCluster, vCluster);
                } else {
                    generateNewEdgeEvents(
                        moat,
                        uPart,
                        vPart,
                        uCluster,
                        vCluster,
                        r,
                        clusterActivity.active(vCluster)
                    );
                }
            }

        }

    }

    private void deactivateCluster(double moat, long clusterId) {
        clusterEventsPriorityQueue.pop();
        clusterStructure.setMoat(clusterId, moat);
        clusterActivity.deactivateCluster(clusterId, moat);
    }

    private void initializeEdgeParts() {
        MutableLong counter = new MutableLong();

        long nodeCount = graph.nodeCount();

        for (long u = 0; u < nodeCount; ++u) {
            graph.forEachRelationship(u, 1.0, (s, t, w) -> {
                var edgeId = counter.getAndIncrement();
                var edgePart1 = 2 * edgeId;
                var edgePart2 = 2 * edgeId + 1;
                edgeCosts.set(edgeId, w);
                edgeParts.set(2 * edgeId, s);
                edgeParts.set(2 * edgeId + 1, t);
                edgeEventsQueue.addBothWays(s, t, edgePart1, edgePart2, w / 2);
                return t > s;
            });
        }
        edgeEventsQueue.performInitialAssignment();
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
        long cluster2
    ) {
        boolean cluster1Inactive = clusterActivity.active(cluster1);
        boolean cluster2Inactive = clusterActivity.active(cluster2);

        if (cluster1Inactive) {
            edgeEventsQueue.increaseValuesOnInactiveCluster(cluster1, moat - clusterActivity.inactiveSince(cluster1));
        } else {
            clusterActivity.deactivateCluster(cluster1, moat);
        }
        if (cluster2Inactive) {
            edgeEventsQueue.increaseValuesOnInactiveCluster(cluster2, moat - clusterActivity.inactiveSince(cluster2));
        } else {
            clusterActivity.deactivateCluster(cluster2, moat);
        }
        var newCluster = clusterStructure.merge(cluster1, cluster2);
        edgeEventsQueue.mergeAndUpdate(newCluster, cluster1, cluster2);
        clusterEventsPriorityQueue.add(newCluster, clusterStructure.tightnessTime(newCluster, moat));

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
            edgeEventsQueue.addWithCheck(uCluster, vPart, moat + r / 2);
            edgeEventsQueue.addWithCheck(vCluster, uPart, moat + r / 2);
        } else {
            edgeEventsQueue.addWithCheck(uCluster, vPart, moat + r);
            edgeEventsQueue.addWithoutCheck(vCluster, uPart, moat);
        }
    }

    private long otherEdgePart(long i) {
        return (i + 1) - 2 * (i % 2);
    }

    private double edgeCost(HugeDoubleArray edgeCosts, long i) {
        return edgeCosts.get(i / 2);
    }
}

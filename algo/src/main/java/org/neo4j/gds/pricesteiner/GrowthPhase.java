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

import java.util.function.LongPredicate;
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
    private final ClusterMoatPair clusterMoatPairOfu = new ClusterMoatPair();
    private final ClusterMoatPair clusterMoatPairOfv = new ClusterMoatPair();
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
        setUpCusterQueue(clusterStructure().active(), clusterStructure.maxActiveCluster());
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

                 clusterStructure.sumOnEdgePart(u,moat,clusterMoatPairOfu);
                 clusterStructure.sumOnEdgePart(v,moat,clusterMoatPairOfv);

                var uCluster = clusterMoatPairOfu.cluster();
                var uClusterSum = clusterMoatPairOfu.totalMoat();

                var vCluster = clusterMoatPairOfv.cluster();
                var vClusterSum = clusterMoatPairOfv.totalMoat();


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

                    edgeCosts.set(edgeId, w);
                    edgeParts.set(2 * edgeId, s);
                    edgeParts.set(2 * edgeId + 1, t);

                    clusterStructure.sumOnEdgePart(s,0.0, clusterMoatPairOfu);
                    clusterStructure.sumOnEdgePart(t,0.0, clusterMoatPairOfv);

                    var cluster1 = clusterMoatPairOfu.cluster();
                    var cluster2 = clusterMoatPairOfv.cluster();

                    if (w>=0) {
                        handlePositiveEdge(edgeId, cluster1, cluster2, w);
                    }else{
                        handleNegativeEdge(edgeId,cluster1,cluster2,w);
                    }
                    return true;
                }
                return false;
            });
            progressTracker.logProgress(graph.degree(u));
        }
        edgeEventsQueue.performInitialAssignment(clusterStructure.maxActiveCluster(),clusterStructure.active());
    }

    private void handlePositiveEdge(long edgeId, long cluster1, long cluster2,  double w){
        if (cluster1!=cluster2) {
            var edgePart1 = 2 * edgeId;
            var edgePart2 = 2 * edgeId + 1;
            edgeEventsQueue.addBothWays(cluster1, cluster2, edgePart1, edgePart2, w / 2.0);
        }

    }
    private void handleNegativeEdge(long edgeId, long cluster1, long cluster2, double w){
            if (cluster1 != cluster2){
                var newCluster =clusterStructure.merge(cluster1,cluster2,0.0);
                edgeEventsQueue.mergeWithoutUpdates(newCluster,cluster1,cluster2);
                clusterStructure.addToInitialMoatLeft(newCluster,-w);
                addToTree(edgeId);
            }
            /*
             * note the following:
             *  assume (a)-[:w1]-(b) and (a)-[:w2]-(b)  with both w1,w2 <0
             *  the algorithm will keep the first one it treats, even though the other one might be preferable (higher prize)
             * Note that the pcst default algorithm by breaking into (a)-(n1)-(b) and (a)-(n2)-(b) can include both n1,n2 for the solution in G, but the transformation
             * does not!
             * In other words, solution(G) can be smaller than solution(G')
             * ---
             * We can do better than this if needed.
             *  One idea is gathering the negative edges, sorting them,  and iterating over them ala prim algorithm etc..
             *  Another is keeping  a dynamic  tree and  replace edges by performing an st query
             * for (a,b) which will identify the worst edge on the path from a to b and replace it with a better one.
             * Noting this for potential future work.
             *.
             */
    }
    private void initializeClusterPrizes() {
        for (long u = 0; u < graph.nodeCount(); ++u) {
            double prize = prizes.applyAsDouble(u);
            clusterStructure.setClusterPrize(u, prize);
        }
    }

    private void setUpCusterQueue(LongPredicate isActive, long maxCount) {
        for (long u = 0; u < maxCount; ++u) {
           if (isActive.test(u)){
               var tightnessTime = clusterStructure.tightnessTime(u,0.0);
               clusterEventsPriorityQueue.add(u,tightnessTime);
           }
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

        var newCluster = clusterStructure.merge(cluster1, cluster2, moat);

        edgeEventsQueue.mergeAndUpdate(newCluster, cluster1, cluster2);
        clusterEventsPriorityQueue.add(newCluster, clusterStructure.tightnessTime(newCluster, moat));

        addToTree(edgeId);
        edgeParts.set(2*edgeId, -edgeParts.get(2*edgeId)); //signal that edge id has been used
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

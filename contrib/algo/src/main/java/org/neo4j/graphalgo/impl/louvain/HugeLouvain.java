/**
 * Copyright (c) 2017 "Neo4j, Inc." <http://neo4j.com>
 *
 * This file is part of Neo4j Graph Algorithms <http://github.com/neo4j-contrib/neo4j-graph-algorithms>.
 *
 * Neo4j Graph Algorithms is free software: you can redistribute it and/or modify
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
package org.neo4j.graphalgo.impl.louvain;

import com.carrotsearch.hppc.BitSet;
import com.carrotsearch.hppc.DoubleArrayList;
import com.carrotsearch.hppc.ObjectArrayList;
import org.neo4j.graphalgo.api.HugeGraph;
import org.neo4j.graphalgo.api.HugeWeightMapping;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeDoubleArray;
import org.neo4j.graphalgo.core.utils.paged.HugeLongArray;
import org.neo4j.graphalgo.core.utils.paged.HugeLongLongDoubleMap;
import org.neo4j.graphalgo.core.write.Exporter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.stream.LongStream;
import java.util.stream.Stream;

/**
 * Louvain Clustering Algorithm.
 * <p>
 * The algo performs modularity optimization as long as the
 * modularity keeps incrementing. Every optimization step leads
 * to an array of length nodeCount containing the nodeId->community mapping.
 * <p>
 * After each step a new graph gets built from the actual community mapping
 * and is used as input for the next step.
 *
 * @author mknblch
 */
public final class HugeLouvain extends LouvainAlgo<HugeLouvain> {

    private final long rootNodeCount;
    private int level;
    private final ExecutorService pool;
    private final int concurrency;
    private final AllocationTracker tracker;
    private ProgressLogger progressLogger;
    private TerminationFlag terminationFlag;
    private HugeLongArray communities;
    private double[] modularities;
    private HugeLongArray[] dendrogram;
    private final HugeDoubleArray nodeWeights;
    private final HugeGraph root;
    private long communityCount;

    public HugeLouvain(
            HugeGraph graph,
            ExecutorService pool,
            int concurrency,
            AllocationTracker tracker) {
        this.root = graph;
        this.pool = pool;
        this.concurrency = concurrency;
        this.tracker = tracker;
        rootNodeCount = graph.nodeCount();
        communities = HugeLongArray.newArray(rootNodeCount, tracker);
        nodeWeights = HugeDoubleArray.newArray(rootNodeCount, tracker);
        communityCount = rootNodeCount;
        communities.setAll(i -> i);
    }

    public HugeLouvain compute(int maxLevel, int maxIterations) {
        return compute(maxLevel, maxIterations, false);
    }

    public HugeLouvain compute(int maxLevel, int maxIterations, boolean rnd) {
        return computeOf(root, rootNodeCount, maxLevel, maxIterations, rnd);
    }

    public HugeLouvain compute(HugeWeightMapping communityMap, int maxLevel, int maxIterations, boolean rnd) {
        BitSet comCount = new BitSet();
        communities.setAll(i -> {
            final long t = (long) communityMap.nodeWeight(i, -1.0);
            final long c = t == -1L ? i : t;
            comCount.set(c);
            return c;
        });
        // temporary graph
        long nodeCount = comCount.cardinality();
        LouvainUtils.normalize(communities);
        HugeGraph graph = rebuildGraph(this.root, communities, nodeCount);

        return computeOf(graph, nodeCount, maxLevel, maxIterations, rnd);
    }

    private HugeLouvain computeOf(
            HugeGraph rootGraph,
            long rootNodeCount,
            int maxLevel,
            int maxIterations,
            boolean rnd) {

        // result arrays, start with small buffers in case we don't require max iterations to converge
        ObjectArrayList<HugeLongArray> dendrogram = new ObjectArrayList<>(0);
        DoubleArrayList modularities = new DoubleArrayList(0);
        long communityCount = this.communityCount;
        long nodeCount = rootNodeCount;
        HugeGraph graph = rootGraph;

        for (int level = 0; level < maxLevel && running(); level++) {
            // start modularity optimization
            final HugeModularityOptimization modularityOptimization =
                    new HugeModularityOptimization(graph,
                            nodeWeights::get,
                            pool,
                            concurrency,
                            tracker, System.currentTimeMillis())
                            .withProgressLogger(progressLogger)
                            .withTerminationFlag(terminationFlag)
                            .withRandomNeighborOptimization(rnd)
                            .compute(maxIterations);
            // rebuild graph based on the community structure
            final HugeLongArray communityIds = modularityOptimization.getCommunityIds();
            communityCount = LouvainUtils.normalize(communityIds);
            progressLogger.log(
                    "level: " + (level + 1) +
                            " communities: " + communityCount +
                            " q: " + modularityOptimization.getModularity());
            if (communityCount >= nodeCount) {
                // release the old algo instance
                modularityOptimization.release();
                break;
            }
            nodeCount = communityCount;
            dendrogram.add(rebuildCommunityStructure(communityIds));
            modularities.add(modularityOptimization.getModularity());
            graph = rebuildGraph(graph, communityIds, communityCount);
            // release the old algo instance
            modularityOptimization.release();
        }
        this.dendrogram = dendrogram.toArray(HugeLongArray.class);
        this.modularities = modularities.toArray();
        this.level = modularities.elementsCount;
        this.communityCount = communityCount;
        return this;
    }

    /**
     * create a virtual graph based on the community structure of the
     * previous louvain round
     *
     * @param graph        previous graph
     * @param communityIds community structure
     * @return a new graph built from a community structure
     */
    private HugeGraph rebuildGraph(HugeGraph graph, HugeLongArray communityIds, long communityCount) {
        // count and normalize community structure
        final long nodeCount = communityIds.size();
        // bag of nodeId->{nodeId, ..}
        SubGraph subGraph = new SubGraph(nodeCount, tracker);
        HugeLongLongDoubleMap relationshipWeights = new HugeLongLongDoubleMap(nodeCount, tracker);
        for (long i = 0L; i < nodeCount; ++i) {
            // map node nodeId to community nodeId
            final long sourceCommunity = communityIds.get(i);
            // get transitions from current node
            graph.forEachOutgoing(i, (s, t) -> {
                // mapping
                final long targetCommunity = communityIds.get(t);
                final double value = graph.weightOf(s, t);
                if (sourceCommunity == targetCommunity) {
                    nodeWeights.addTo(sourceCommunity, value);
                }
                // add IN and OUT relation
                subGraph.add(targetCommunity, sourceCommunity);
                subGraph.add(sourceCommunity, targetCommunity);

                relationshipWeights.addTo(sourceCommunity, targetCommunity, value / 2.0); // TODO validate
                relationshipWeights.addTo(targetCommunity, sourceCommunity, value / 2.0);
                return true;
            });

        }

        if (graph instanceof HugeLouvainGraph) {
            graph.release();
        }

        // create temporary graph
        return new HugeLouvainGraph(communityCount, subGraph, relationshipWeights);
    }

    private HugeLongArray rebuildCommunityStructure(HugeLongArray communityIds) {
        // rebuild community array
        this.communities.setAll(i -> communityIds.get(communities.get(i)));
        // the communities are stored in the dendrogram, one per level
        // so we have to copy the current state and return it as a snapshot
        HugeLongArray copy = HugeLongArray.newArray(rootNodeCount, tracker);
        this.communities.copyTo(copy, rootNodeCount);
        return copy;
    }

    /**
     * nodeId to community mapping array
     *
     * @return
     */
    public HugeLongArray getCommunityIds() {
        return communities;
    }

    public HugeLongArray[] getDendrogram() {
        return dendrogram;
    }

    @Override
    public double[] getModularities() {
        return Arrays.copyOfRange(modularities, 0, level);
    }

    /**
     * number of outer iterations
     *
     * @return
     */
    @Override
    public int getLevel() {
        return level;
    }

    /**
     * number of distinct communities
     *
     * @return
     */
    public long getCommunityCount() {
        return communityCount;
    }

    @Override
    public long communityIdOf(final long node) {
        return communities.get(node);
    }

    /**
     * result stream
     *
     * @return
     */
    public Stream<Result> resultStream() {
        return LongStream.range(0L, rootNodeCount)
                .mapToObj(i -> new Result(i, communities.get(i)));
    }

    public Stream<StreamingResult> dendrogramStream(boolean includeIntermediateCommunities) {
        return LongStream.range(0L, rootNodeCount)
                .mapToObj(i -> {
                    List<Long> communitiesList = null;
                    if (includeIntermediateCommunities) {
                        communitiesList = new ArrayList<>(dendrogram.length);
                        for (HugeLongArray community : dendrogram) {
                            communitiesList.add(community.get(i));
                        }
                    }

                    return new StreamingResult(root.toOriginalNodeId(i), communitiesList, communities.get(i));
                });
    }

    @Override
    public void export(
            Exporter exporter,
            String propertyName,
            boolean includeIntermediateCommunities,
            String intermediateCommunitiesPropertyName) {
        if (includeIntermediateCommunities) {
            exporter.write(
                    propertyName,
                    communities,
                    HugeLongArray.Translator.INSTANCE,
                    intermediateCommunitiesPropertyName,
                    dendrogram,
                    HUGE_COMMUNITIES_TRANSLATOR
            );
        } else {
            exporter.write(
                    propertyName,
                    communities,
                    HugeLongArray.Translator.INSTANCE
            );
        }
    }

    @Override
    public HugeLouvain release() {
        tracker.remove(communities.release());
        communities = null;
        return this;
    }

    @Override
    public HugeLouvain withProgressLogger(ProgressLogger progressLogger) {
        this.progressLogger = progressLogger;
        return this;
    }

    @Override
    public HugeLouvain withTerminationFlag(TerminationFlag terminationFlag) {
        this.terminationFlag = terminationFlag;
        return this;
    }
}

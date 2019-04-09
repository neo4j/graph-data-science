/*
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

import com.carrotsearch.hppc.IntObjectMap;
import com.carrotsearch.hppc.IntObjectScatterMap;
import com.carrotsearch.hppc.IntScatterSet;
import com.carrotsearch.hppc.LongDoubleScatterMap;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.WeightMapping;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.RawValues;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.write.Exporter;
import org.neo4j.graphalgo.core.write.Translators;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.stream.IntStream;
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
public final class Louvain extends LouvainAlgo<Louvain> {

    private final int rootNodeCount;
    private int level;
    private final ExecutorService pool;
    private final int concurrency;
    private final AllocationTracker tracker;
    private ProgressLogger progressLogger;
    private TerminationFlag terminationFlag;
    private int[] communities;
    private double[] modularities;
    private int[][] dendrogram;
    private double[] nodeWeights;
    private Graph root;
    private int communityCount;

    public Louvain(Graph graph,
                   ExecutorService pool,
                   int concurrency,
                   AllocationTracker tracker) {
        this.root = graph;
        this.pool = pool;
        this.concurrency = concurrency;
        this.tracker = tracker;
        rootNodeCount = Math.toIntExact(graph.nodeCount());
        communities = new int[rootNodeCount];
        nodeWeights = new double[rootNodeCount];
        tracker.add(4 * rootNodeCount);
        communityCount = rootNodeCount;
        Arrays.setAll(communities, i -> i);
    }

    public Louvain compute(int maxLevel, int maxIterations) {
        return compute(maxLevel, maxIterations, false);
    }

    public Louvain compute(int maxLevel, int maxIterations, boolean rnd) {
        // temporary graph
        Graph graph = this.root;
        // result arrays
        dendrogram = new int[maxLevel][];
        modularities = new double[maxLevel];
        int nodeCount = rootNodeCount;
        for (level = 0; level < maxLevel; level++) {
            // start modularity optimization
            final ModularityOptimization modularityOptimization =
                    new ModularityOptimization(graph,
                            nodeId -> nodeWeights[nodeId],
                            pool,
                            concurrency,
                            tracker, System.currentTimeMillis())
                            .withProgressLogger(progressLogger)
                            .withTerminationFlag(terminationFlag)
                            .withRandomNeighborOptimization(rnd)
                            .compute(maxIterations);
            // rebuild graph based on the community structure
            final int[] communityIds = modularityOptimization.getCommunityIds();
            communityCount = LouvainUtils.normalize(communityIds);
            // release the old algo instance
            modularityOptimization.release();
            progressLogger.log(
                    "level: " + (level + 1) +
                            " communities: " + communityCount +
                            " q: " + modularityOptimization.getModularity());
            if (communityCount >= nodeCount) {
                break;
            }
            nodeCount = communityCount;
            dendrogram[level] = rebuildCommunityStructure(communityIds);
            modularities[level] = modularityOptimization.getModularity();
            graph = rebuildGraph(graph, communityIds, communityCount);
        }
        dendrogram = Arrays.copyOf(dendrogram, level);
        return this;
    }

    public Louvain compute(WeightMapping communityMap, int maxLevel, int maxIterations, boolean rnd) {
        BitSet comCount = new BitSet();
        Arrays.setAll(communities, i -> {
            final int t = (int) communityMap.get(i, -1.0);
            final int c = t == -1 ? i : t;
            comCount.set(c);
            return c;
        });
        // temporary graph
        int nodeCount = comCount.cardinality();
        LouvainUtils.normalize(communities);
        Graph graph = rebuildGraph(this.root, communities, nodeCount);
        // result arrays
        dendrogram = new int[maxLevel][];
        modularities = new double[maxLevel];

        for (level = 0; level < maxLevel && terminationFlag.running(); level++) {
            // start modularity optimization
            final ModularityOptimization modularityOptimization =
                    new ModularityOptimization(graph,
                            nodeId -> nodeWeights[nodeId],
                            pool,
                            concurrency,
                            tracker, System.currentTimeMillis())
                            .withProgressLogger(progressLogger)
                            .withTerminationFlag(terminationFlag)
                            .withRandomNeighborOptimization(rnd)
                            .compute(maxIterations);
            // rebuild graph based on the community structure
            final int[] communityIds = modularityOptimization.getCommunityIds();
            communityCount = LouvainUtils.normalize(communityIds);
            // release the old algo instance
            modularityOptimization.release();
            progressLogger.log(
                    "level: " + (level + 1) +
                            " communities: " + communityCount +
                            " q: " + modularityOptimization.getModularity());
            if (communityCount >= nodeCount) {
                break;
            }
            nodeCount = communityCount;
            dendrogram[level] = rebuildCommunityStructure(communityIds);
            modularities[level] = modularityOptimization.getModularity();
            graph = rebuildGraph(graph, communityIds, communityCount);
        }
        dendrogram = Arrays.copyOf(dendrogram, level);
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
    private Graph rebuildGraph(Graph graph, int[] communityIds, int communityCount) {
        // count and normalize community structure
        final int nodeCount = communityIds.length;
        // bag of nodeId->{nodeId, ..}
        final IntObjectMap<IntScatterSet> relationships = new IntObjectScatterMap<>(nodeCount);
        // accumulated weights
        final LongDoubleScatterMap relationshipWeights = new LongDoubleScatterMap(nodeCount);
        // for each node in the current graph
        for (int i = 0; i < nodeCount; i++) {
            // map node nodeId to community nodeId
            final int sourceCommunity = communityIds[i];
            // get transitions from current node
            graph.forEachOutgoing(i, (s, t, r) -> {
                // mapping
                final int targetCommunity = communityIds[t];
                final double value = graph.weightOf(s, t);
                if (sourceCommunity == targetCommunity) {
                    nodeWeights[sourceCommunity] += value;
                }
                // add IN and OUT relation
                putIfAbsent(relationships, targetCommunity).add(sourceCommunity);
                putIfAbsent(relationships, sourceCommunity).add(targetCommunity);
                relationshipWeights.addTo(RawValues.combineIntInt(sourceCommunity, targetCommunity), value / 2); // TODO validate
                relationshipWeights.addTo(RawValues.combineIntInt(targetCommunity, sourceCommunity), value / 2);
                return true;
            });
        }

        // create temporary graph
        return new LouvainGraph(communityCount, relationships, relationshipWeights);
    }

    private int[] rebuildCommunityStructure(int[] communityIds) {
        // rebuild community array
        assert rootNodeCount == communities.length;
        final int[] ints = new int[rootNodeCount];
        Arrays.setAll(ints, i -> communityIds[communities[i]]);
        communities = ints;
        return communities;
    }

    /**
     * nodeId to community mapping array
     *
     * @return
     */
    public int[] getCommunityIds() {
        return communities;
    }

    public int[][] getDendrogram() {
        return dendrogram;
    }

    @Override
    public double[] getModularities() {
        return Arrays.copyOfRange(modularities, 0, level);
    }

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
        return communities[(int) node];
    }

    /**
     * result stream
     *
     * @return
     */
    public Stream<Result> resultStream() {
        return IntStream.range(0, rootNodeCount)
                .mapToObj(i -> new Result(i, communities[i]));
    }

    public Stream<StreamingResult> dendrogramStream(boolean includeIntermediateCommunities) {
        return IntStream.range(0, rootNodeCount)
                .mapToObj(i -> {
                    List<Long> communitiesList = null;
                    if (includeIntermediateCommunities) {
                        communitiesList = new ArrayList<>(dendrogram.length);
                        for (int[] community : dendrogram) {
                            communitiesList.add((long) community[i]);
                        }
                    }

                    return new StreamingResult(root.toOriginalNodeId(i), communitiesList, communities[i]);
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
                    Translators.INT_ARRAY_TRANSLATOR,
                    intermediateCommunitiesPropertyName,
                    dendrogram,
                    COMMUNITIES_TRANSLATOR
            );
        } else {
            exporter.write(
                    propertyName,
                    communities,
                    Translators.INT_ARRAY_TRANSLATOR
            );
        }
    }

    @Override
    public Louvain release() {
        tracker.remove(4 * rootNodeCount);
        communities = null;
        return this;
    }

    @Override
    public Louvain withProgressLogger(ProgressLogger progressLogger) {
        this.progressLogger = progressLogger;
        return this;
    }

    @Override
    public Louvain withTerminationFlag(TerminationFlag terminationFlag) {
        this.terminationFlag = terminationFlag;
        return this;
    }

    private static IntScatterSet putIfAbsent(IntObjectMap<IntScatterSet> relationships, int community) {
        final IntScatterSet intCursors = relationships.get(community);
        if (null == intCursors) {
            final IntScatterSet newSet = new IntScatterSet();
            relationships.put(community, newSet);
            return newSet;
        }
        return intCursors;
    }
}

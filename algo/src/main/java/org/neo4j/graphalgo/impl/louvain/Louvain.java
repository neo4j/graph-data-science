/*
 * Copyright (c) 2017-2019 "Neo4j,"
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
package org.neo4j.graphalgo.impl.louvain;

import com.carrotsearch.hppc.BitSet;
import com.carrotsearch.hppc.DoubleArrayList;
import com.carrotsearch.hppc.ObjectArrayList;
import org.neo4j.graphalgo.Algorithm;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.NodeOrRelationshipProperties;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeCursor;
import org.neo4j.graphalgo.core.utils.paged.HugeDoubleArray;
import org.neo4j.graphalgo.core.utils.paged.HugeLongArray;
import org.neo4j.graphalgo.core.write.Exporter;
import org.neo4j.graphalgo.core.write.PropertyTranslator;
import org.neo4j.graphalgo.impl.utils.CommunityUtils;
import org.neo4j.graphdb.Direction;
import org.neo4j.values.storable.Values;

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
public final class Louvain extends Algorithm<Louvain> {

    public static final double DEFAULT_WEIGHT = 1.0;

    private static final PropertyTranslator<HugeLongArray[]> HUGE_COMMUNITIES_TRANSLATOR =
            (propertyId, allCommunities, nodeId) -> {
                // build int array
                final long[] data = new long[allCommunities.length];
                for (int i = 0; i < data.length; i++) {
                    data[i] = allCommunities[i].get(nodeId);
                }
                return Values.longArray(data);
            };

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
    private final Graph root;
    private long communityCount;

    private final NodeOrRelationshipProperties communityMap;
    private final int maxLevel;
    private final int maxIterations;

    public Louvain(
            final Graph graph,
            final Config config,
            final ExecutorService pool,
            final int concurrency,
            final AllocationTracker tracker) {this(graph, config, null, pool, concurrency, tracker);}

    public Louvain(
            final Graph graph,
            final Config config,
            final NodeOrRelationshipProperties communityMap,
            final ExecutorService pool,
            final int concurrency,
            final AllocationTracker tracker) {
        this.root = graph;
        this.pool = pool;
        this.concurrency = concurrency;
        this.tracker = tracker;
        rootNodeCount = graph.nodeCount();
        communities = HugeLongArray.newArray(rootNodeCount, tracker);
        nodeWeights = HugeDoubleArray.newArray(rootNodeCount, tracker);
        this.communityMap = communityMap;
        maxLevel = config.maxLevel;
        maxIterations = config.maxIterations;
        communityCount = rootNodeCount;
        communities.setAll(i -> i);
    }

    public Louvain compute() {
        if (communityMap != null) {
            return compute(communityMap, maxLevel, maxIterations);
        } else {
            return compute(maxLevel, maxIterations);
        }
    }

    public Louvain compute(final int maxLevel, final int maxIterations) {
        return computeOf(root, rootNodeCount, maxLevel, maxIterations);
    }

    public Louvain compute(
            final NodeOrRelationshipProperties communityMap,
            final int maxLevel,
            final int maxIterations) {
        BitSet comCount = new BitSet();
        communities.setAll(i -> {
            final long c = (long) communityMap.nodeProperty(i, i);
            comCount.set(c);
            return c;
        });
        // temporary graph
        long nodeCount = comCount.cardinality();
        CommunityUtils.normalize(communities);
        Graph graph = rebuildGraph(this.root, communities, nodeCount);

        return computeOf(graph, nodeCount, maxLevel, maxIterations);
    }

    private Louvain computeOf(
            final Graph rootGraph,
            final long rootNodeCount,
            final int maxLevel,
            final int maxIterations) {

        // result arrays, start with small buffers in case we don't require max iterations to converge
        ObjectArrayList<HugeLongArray> dendrogram = new ObjectArrayList<>(0);
        DoubleArrayList modularities = new DoubleArrayList(0);
        long communityCount = this.communityCount;
        long nodeCount = rootNodeCount;
        Graph graph = rootGraph;

        for (int level = 0; level < maxLevel; level++) {
            assertRunning();
            // start modularity optimization
            final ModularityOptimization modularityOptimization =
                    new ModularityOptimization(
                            graph,
                            nodeWeights::get,
                            pool,
                            concurrency,
                            tracker
                    )
                            .withProgressLogger(progressLogger)
                            .withTerminationFlag(terminationFlag)
                            .compute(maxIterations);
            // rebuild graph based on the community structure
            final HugeLongArray communityIds = modularityOptimization.getCommunityIds();
            communityCount = CommunityUtils.normalize(communityIds);
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

    private static final int MAX_MAP_ENTRIES = (int) ((Integer.MAX_VALUE - 2) * 0.75F);

    /**
     * create a virtual graph based on the community structure of the
     * previous louvain round
     *
     * @param graph        previous graph
     * @param communityIds community structure
     * @return a new graph built from a community structure
     */
    private Graph rebuildGraph(final Graph graph, final HugeLongArray communityIds, final long communityCount) {
        if (communityCount < MAX_MAP_ENTRIES) {
            return rebuildSmallerGraph(graph, communityIds, (int) communityCount);
        }

        HugeDoubleArray nodeWeights = this.nodeWeights;

        // bag of nodeId->{nodeId, ..}
        LongLongSubGraph subGraph = new LongLongSubGraph(communityCount, graph.hasRelationshipProperty(), tracker);

        // for each node in the current graph
        HugeCursor<long[]> cursor = communityIds.initCursor(communityIds.newCursor());
        while (cursor.next()) {
            long[] communities = cursor.array;
            int start = cursor.offset;
            int end = cursor.limit;
            long base = cursor.base;

            while (start < end) {
                // map node nodeId to community nodeId
                final long sourceCommunity = communities[start];

                // get transitions from current node
                graph.forEachRelationship(base + start, Direction.OUTGOING, DEFAULT_WEIGHT, (s, t, w) -> {
                    // mapping
                    final long targetCommunity = communityIds.get(t);
                    if (sourceCommunity == targetCommunity) {
                        nodeWeights.addTo(sourceCommunity, w);
                    }

                    // add IN and OUT relation and weights
                    subGraph.add(sourceCommunity, targetCommunity, (float) (w / 2.0)); // TODO validate
                    return true;
                });

                ++start;
            }
        }

        if (graph instanceof SubGraph) {
            graph.release();
        }

        // create temporary graph
        return subGraph;
    }

    private Graph rebuildSmallerGraph(
            final Graph graph,
            final HugeLongArray communityIds,
            final int communityCount) {
        HugeDoubleArray nodeWeights = this.nodeWeights;

        // bag of nodeId->{nodeId, ..}
        final IntIntSubGraph subGraph = new IntIntSubGraph(communityCount, graph.hasRelationshipProperty());

        // for each node in the current graph
        HugeCursor<long[]> cursor = communityIds.initCursor(communityIds.newCursor());
        while (cursor.next()) {
            long[] communities = cursor.array;
            int start = cursor.offset;
            int end = cursor.limit;
            long base = cursor.base;

            while (start < end) {
                // map node nodeId to community nodeId
                final int sourceCommunity = (int) communities[start];

                // get transitions from current node
                graph.forEachRelationship(base + start, Direction.OUTGOING, DEFAULT_WEIGHT, (s, t, value) -> {
                    // mapping
                    final int targetCommunity = (int) communityIds.get(t);
                    if (sourceCommunity == targetCommunity) {
                        nodeWeights.addTo(sourceCommunity, value);
                    }

                    // add IN and OUT relation and weights
                    subGraph.add(sourceCommunity, targetCommunity, (float) (value / 2.0)); // TODO validate
                    return true;
                });

                ++start;
            }
        }

        if (graph instanceof SubGraph) {
            graph.release();
        }

        // create temporary graph
        return subGraph;
    }

    private HugeLongArray rebuildCommunityStructure(final HugeLongArray communityIds) {
        // rebuild community array
        try (HugeCursor<long[]> cursor = communities.initCursor(communities.newCursor())) {
            while (cursor.next()) {
                long[] array = cursor.array;
                int limit = Math.min(cursor.limit, array.length);
                for (int i = cursor.offset; i < limit; ++i) {
                    array[i] = communityIds.get(array[i]);
                }
            }
        }
        // the communities are stored in the dendrogram, one per level
        // so we have to copy the current state and return it as a snapshot
        return communities.copyOf(rootNodeCount, tracker);
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

    public double[] getModularities() {
        return Arrays.copyOfRange(modularities, 0, level);
    }

    /**
     * number of outer iterations
     *
     * @return
     */
    public int getLevel() {
        return level;
    }

    /**
     * number of distinct communities
     *
     * @return
     */
    public long communityCount() {
        return communityCount;
    }

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

    public Stream<StreamingResult> dendrogramStream(final boolean includeIntermediateCommunities) {
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

    public void export(
            final Exporter exporter,
            final String propertyName,
            final boolean includeIntermediateCommunities,
            final String intermediateCommunitiesPropertyName) {
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
    public void release() {
        tracker.remove(communities.release());
        communities = null;
    }

    @Override
    public Louvain withProgressLogger(final ProgressLogger progressLogger) {
        this.progressLogger = progressLogger;
        return this;
    }

    @Override
    public Louvain withTerminationFlag(final TerminationFlag terminationFlag) {
        this.terminationFlag = terminationFlag;
        return this;
    }

    public double getFinalModularity() {
        double[] modularities = getModularities();
        return modularities.length > 0 ? modularities[modularities.length - 1] : 0.0D;
    }

    @Override
    public final Louvain me() {
        return this;
    }

    /**
     * result object
     */
    public static final class Result {

        public final long nodeId;
        public final long community;

        public Result(final long id, final long community) {
            this.nodeId = id;
            this.community = community;
        }
    }

    public static final class StreamingResult {
        public final long nodeId;
        public final List<Long> communities;
        public final long community;

        StreamingResult(final long nodeId, final List<Long> communities, final long community) {
            this.nodeId = nodeId;
            this.communities = communities;
            this.community = community;
        }
    }

    public static class Config {

        public final NodeOrRelationshipProperties communityMap;
        public final int maxLevel;
        public final int maxIterations;
        public final boolean randomNeighborSelection;

        public Config(final int maxLevel) {
            this(maxLevel, maxLevel);
        }

        public Config(
                final int maxLevel,
                final int maxIterations) {
            this(null, maxLevel, maxIterations, false);
        }

        public Config(
                final int maxLevel,
                final int maxIterations,
                final boolean randomNeighborSelection) {
            this(null, maxLevel, maxIterations, randomNeighborSelection);
        }

        public Config(
                final NodeOrRelationshipProperties communityMap,
                final int maxLevel,
                final int maxIterations,
                final boolean randomNeighborSelection) {
            this.communityMap = communityMap;
            this.maxLevel = maxLevel;
            this.maxIterations = maxIterations;
            this.randomNeighborSelection = randomNeighborSelection;
        }
    }
}

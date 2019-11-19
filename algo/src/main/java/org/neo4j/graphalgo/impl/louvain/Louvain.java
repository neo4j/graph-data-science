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

import org.apache.commons.lang3.mutable.MutableLong;
import org.neo4j.graphalgo.Algorithm;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.NodeProperties;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeLongArray;
import org.neo4j.graphalgo.impl.modularity.ModularityOptimization;
import org.neo4j.graphdb.Direction;
import org.neo4j.logging.NullLog;

import java.util.Optional;
import java.util.concurrent.ExecutorService;

import static org.neo4j.graphalgo.core.ProcedureConstants.TOLERANCE_DEFAULT;
import static org.neo4j.graphalgo.core.utils.ParallelUtil.DEFAULT_BATCH_SIZE;

public final class Louvain extends Algorithm<Louvain> {

    private final NodeProperties seedingValues;
    private final Direction direction;
    private final ExecutorService pool;
    private final int concurrency;
    private final AllocationTracker tracker;
    private final Graph rootGraph;
    private final Config config;

    // results
    private HugeLongArray[] dendrograms;
    private double[] modularities;
    private int ranLevels;

    public Louvain(
        Graph graph,
        Config config,
        Direction direction,
        ExecutorService pool,
        int concurrency,
        AllocationTracker tracker
    ) {
        this.config = config;
        this.rootGraph = graph;
        this.seedingValues = config.maybeSeedPropertyKey.map(graph::nodeProperties).orElse(null);
        this.direction = direction;
        this.pool = pool;
        this.concurrency = concurrency;
        this.tracker = tracker;
        this.dendrograms = new HugeLongArray[config.maxLevel];
        this.modularities = new double[config.maxLevel];
    }

    public Louvain compute() {

        Graph workingGraph = rootGraph;
        NodeProperties seed = seedingValues;

        long oldNodeCount = rootGraph.nodeCount();
        for (ranLevels = 0; ranLevels < config.maxLevel; ranLevels++) {
            ModularityOptimization modularityOptimization = runModularityOptimization(workingGraph, seed);
            modularityOptimization.release();

            modularities[ranLevels] = modularityOptimization.getModularity();
            dendrograms[ranLevels] = HugeLongArray.newArray(rootGraph.nodeCount(), tracker);
            long maxCommunityId = buildDendrogram(workingGraph, ranLevels, modularityOptimization);

            workingGraph = summarizeGraph(workingGraph, modularityOptimization, maxCommunityId);
            seed = new OriginalIdNodeProperties(workingGraph);

            if (workingGraph.nodeCount() == oldNodeCount || workingGraph.nodeCount() == 1) {
                resizeResultArrays();
                break;
            }
            oldNodeCount = workingGraph.nodeCount();
        }

        return this;
    }

    private void resizeResultArrays() {
        int numLevels = levels();
        HugeLongArray[] resizedDendrogram = new HugeLongArray[numLevels];
        double[] resizedModularities = new double[numLevels];
        if (numLevels < this.dendrograms.length) {
            System.arraycopy(this.dendrograms, 0, resizedDendrogram, 0, numLevels);
            System.arraycopy(this.modularities, 0, resizedModularities, 0, numLevels);
        }
        this.dendrograms = resizedDendrogram;
        this.modularities = resizedModularities;
    }

    private long buildDendrogram(
        Graph workingGraph,
        int level,
        ModularityOptimization modularityOptimization
    ) {
        MutableLong maxCommunityId = new MutableLong(0L);
        ParallelUtil.parallelForEachNode(rootGraph, (nodeId) -> {
            long prevId = level == 0
                ? nodeId
                : workingGraph.toMappedNodeId(dendrograms[level - 1].get(nodeId));

            final long communityId = modularityOptimization.getCommunityId(prevId);
            if (communityId > maxCommunityId.getValue()) {
                maxCommunityId.setValue(communityId);
            }
            dendrograms[level].set(nodeId, communityId);
        });

        return maxCommunityId.getValue();
    }

    private ModularityOptimization runModularityOptimization(Graph louvainGraph, NodeProperties seed) {
        return new ModularityOptimization(
            louvainGraph,
            direction,
            10,
            TOLERANCE_DEFAULT,
            seed,
            concurrency,
            DEFAULT_BATCH_SIZE,
            pool,
            tracker,
            NullLog.getInstance()
        )
            .withProgressLogger(progressLogger)
            .withTerminationFlag(terminationFlag)
            .compute();
    }

    private Graph summarizeGraph(Graph workingGraph, ModularityOptimization modularityOptimization, long maxCommunityId) {
        SubGraphGenerator.NodeImporter nodeImporter = new SubGraphGenerator.NodeImporter(
            workingGraph.nodeCount(),
            maxCommunityId,
            direction,
            rootGraph.isUndirected(),
            true,
            tracker
        );

        workingGraph.forEachNode((nodeId) -> {
            nodeImporter.addNode(modularityOptimization.getCommunityId(nodeId));
            return true;
        });

        SubGraphGenerator.RelImporter relImporter = nodeImporter.build();

        workingGraph.forEachNode((nodeId) -> {
            long communityId = modularityOptimization.getCommunityId(nodeId);
            workingGraph.forEachRelationship(nodeId, direction, 1.0, (s, t, w) -> {
                relImporter.add(communityId, modularityOptimization.getCommunityId(t), w);
                return true;
            });
            return true;
        });

        return relImporter.build();
    }

    public Config config() {
        return this.config;
    }

    public HugeLongArray[] dendrograms() {
        return this.dendrograms;
    }

    public HugeLongArray finalDendrogram() {
        return this.dendrograms[levels() - 1];
    }

    public long getCommunity(long nodeId) {
        return dendrograms[ranLevels].get(nodeId);
    }

    public long[] getCommunities(long nodeId) {
        long[] communities = new long[dendrograms.length];

        for (int i = 0; i < dendrograms.length; i++) {
            communities[i] = dendrograms[i].get(nodeId);
        }

        return communities;
    }

    public int levels() {
        return this.ranLevels == 0 ? 1 : this.ranLevels;
    }

    public double[] modularities() {
        return this.modularities;
    }

    @Override
    public void release() {
        // TODO implement
    }

    @Override
    public Louvain me() {
        return this;
    }

    class OriginalIdNodeProperties implements NodeProperties {
        private final Graph graph;

        public OriginalIdNodeProperties(Graph graph) {
            this.graph = graph;
        }

        @Override
        public double nodeProperty(long nodeId) {
            return graph.toOriginalNodeId(nodeId);
        }
    }

    public static class Config {
        public final int maxLevel;
        public final int maxInnerIterations;
        public final double tolerance;
        public final boolean includeIntermediateCommunities;
        public final Optional<String> maybeSeedPropertyKey;

        public Config(
            int maxLevel,
            int maxInnerIterations,
            double tolerance,
            boolean includeIntermediateCommunities,
            Optional<String> maybeSeedPropertyKey
        ) {
            this.maxLevel = maxLevel;
            this.maxInnerIterations = maxInnerIterations;
            this.tolerance = tolerance;
            this.includeIntermediateCommunities = includeIntermediateCommunities;
            this.maybeSeedPropertyKey = maybeSeedPropertyKey;
        }

    }
}

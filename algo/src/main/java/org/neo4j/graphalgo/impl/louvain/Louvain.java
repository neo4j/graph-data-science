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

import org.neo4j.graphalgo.Algorithm;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.NodeProperties;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.utils.ProgressTimer;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeLongArray;
import org.neo4j.graphalgo.impl.modularity.ModularityOptimization;
import org.neo4j.graphdb.Direction;
import org.neo4j.logging.Log;

import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;

import static org.neo4j.graphalgo.core.utils.ParallelUtil.DEFAULT_BATCH_SIZE;

public final class Louvain extends Algorithm<Louvain> {

    private final int concurrency;
    private final Graph rootGraph;
    private final Config config;
    private final NodeProperties seedingValues;
    private Direction direction;
    private final ExecutorService executorService;
    private final Log log;
    private final AllocationTracker tracker;

    // results
    private HugeLongArray[] dendrograms;
    private double[] modularities;
    private int ranLevels;

    public Louvain(
        Graph graph,
        Config config,
        Direction direction,
        ExecutorService executorService,
        int concurrency,
        Log log,
        AllocationTracker tracker
    ) {
        this.config = config;
        this.rootGraph = graph;
        this.log = log;
        this.seedingValues = config.maybeSeedPropertyKey.map(graph::nodeProperties).orElse(null);
        this.direction = direction;
        this.executorService = executorService;
        this.concurrency = concurrency;
        this.tracker = tracker;
        this.dendrograms = new HugeLongArray[config.maxLevel];
        this.modularities = new double[config.maxLevel];
    }

    public void setDirection(Direction direction) {
        this.direction = direction;
    }

    public Louvain compute() {

        Graph workingGraph = rootGraph;
        NodeProperties nextSeedingValues = seedingValues;

        long oldNodeCount = rootGraph.nodeCount();
        for (ranLevels = 0; ranLevels < config.maxLevel; ranLevels++) {
            try (ProgressTimer timer = ProgressTimer.start(millis -> log.info("Louvain - Level %d finished after %dms", ranLevels + 1, millis)))  {

                assertRunning();

                ModularityOptimization modularityOptimization = runModularityOptimization(
                    workingGraph,
                    nextSeedingValues
                );
                modularityOptimization.release();

                modularities[ranLevels] = modularityOptimization.getModularity();
                dendrograms[ranLevels] = HugeLongArray.newArray(rootGraph.nodeCount(), tracker);
                long maxCommunityId = buildDendrogram(workingGraph, ranLevels, modularityOptimization);

                workingGraph = summarizeGraph(workingGraph, modularityOptimization, maxCommunityId);
                nextSeedingValues = new OriginalIdNodeProperties(workingGraph);
            }

            if (workingGraph.nodeCount() == oldNodeCount
                || workingGraph.nodeCount() == 1
                || hasConverged()
            ) {
                resizeResultArrays();
                log.info("Louvain - Finished after %d levels", levels());
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
        AtomicLong maxCommunityId = new AtomicLong(0L);
        ParallelUtil.parallelForEachNode(rootGraph, (nodeId) -> {
            long prevId = level == 0
                ? nodeId
                : workingGraph.toMappedNodeId(dendrograms[level - 1].get(nodeId));

            final long communityId = modularityOptimization.getCommunityId(prevId);
            maxCommunityId.updateAndGet(currentMaxId -> Math.max(communityId, currentMaxId));
            dendrograms[level].set(nodeId, communityId);
        });

        return maxCommunityId.get();
    }

    private ModularityOptimization runModularityOptimization(Graph louvainGraph, NodeProperties seed) {
        return new ModularityOptimization(
            louvainGraph,
            direction,
            10,
            config.tolerance,
            seed,
            concurrency,
            DEFAULT_BATCH_SIZE,
            executorService,
            tracker,
            log
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

        assertRunning();

        workingGraph.forEachNode((nodeId) -> {
            nodeImporter.addNode(modularityOptimization.getCommunityId(nodeId));
            return true;
        });

        assertRunning();

        SubGraphGenerator.RelImporter relImporter = nodeImporter.build();

        workingGraph.forEachNode((nodeId) -> {
            long communityId = modularityOptimization.getCommunityId(nodeId);
            workingGraph.forEachRelationship(nodeId, direction, 1.0, (source, target, property) -> {
                relImporter.add(communityId, modularityOptimization.getCommunityId(target), property);
                return true;
            });
            return true;
        });

        return relImporter.build();
    }

    private boolean hasConverged() {
        if (ranLevels == 0) {
            return false;
        }

        double previousModularity = modularities[ranLevels - 1];
        double currentModularity = modularities[ranLevels];
        return !(currentModularity > previousModularity && Math.abs(currentModularity - previousModularity) > config.tolerance);
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
        return dendrograms[levels() - 1].get(nodeId);
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
        this.rootGraph.releaseTopology();
    }

    @Override
    public Louvain me() {
        return this;
    }

    static class OriginalIdNodeProperties implements NodeProperties {
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

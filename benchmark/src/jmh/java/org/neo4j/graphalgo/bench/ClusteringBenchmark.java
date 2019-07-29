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
package org.neo4j.graphalgo.bench;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.huge.loader.HugeGraphFactory;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.helper.graphbuilder.GraphBuilder;
import org.neo4j.graphalgo.impl.infomap.InfoMap;
import org.neo4j.graphalgo.impl.louvain.Louvain;
import org.neo4j.graphalgo.impl.pagerank.PageRank;
import org.neo4j.graphalgo.impl.pagerank.PageRankAlgorithmType;
import org.neo4j.graphalgo.impl.results.CentralityResult;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

import java.util.concurrent.TimeUnit;
import java.util.stream.LongStream;

/**
 * @author mknblch
 */
@Threads(1)
@Fork(value = 1, jvmArgs = {"-Xms2g", "-Xmx2g"})
@Warmup(iterations = 3, time = 3)
@Measurement(iterations = 3, time = 3)
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class ClusteringBenchmark {

    private static final String LABEL = "Node";
    private static final String RELATIONSHIP = "REL";

    private Graph g;
    private GraphDatabaseAPI api;

    @Param({"0.1", "0.25", "0.5"})
    private double connectedness;

    @Param({"1", "4", "8"})
    private int concurrency;

    @Param({"200"})
    private int nodeCount;

    private CentralityResult pageRankResult;

    @Setup
    public void setup() {
        api = (GraphDatabaseAPI)
                new TestGraphDatabaseFactory()
                        .newImpermanentDatabaseBuilder()
                        .newGraphDatabase();

        GraphBuilder.create(api)
                .setLabel(LABEL)
                .setRelationship(RELATIONSHIP)
                .newCompleteGraphBuilder()
                .createCompleteGraph(nodeCount, connectedness);

        g = new GraphLoader(api)
                .withLabel(LABEL)
                .withRelationshipType(RELATIONSHIP)
                .withoutRelationshipWeights()
                .withoutNodeWeights()
                .sorted()
                .undirected()
                .load(HugeGraphFactory.class);

        final PageRank.Config algoConfig = new PageRank.Config(10, 1.0 - InfoMap.TAU, PageRank.DEFAULT_TOLERANCE);
        pageRankResult = PageRankAlgorithmType.NON_WEIGHTED
                .create(g, algoConfig, LongStream.empty())
                .compute()
                .result();
    }


    @TearDown
    public void tearDown() {
        if (api != null) api.shutdown();
        Pools.DEFAULT.shutdownNow();
    }

    @Benchmark
    public Object _01_louvain() {
        Louvain.Config algoConfig = new Louvain.Config(99, 99999);
        return new Louvain(g, algoConfig, Pools.DEFAULT, concurrency, AllocationTracker.EMPTY)
                .withProgressLogger(ProgressLogger.NULL_LOGGER)
                .withTerminationFlag(TerminationFlag.RUNNING_TRUE)
                .compute()
                .communityCount();
    }

    @Benchmark
    public Object _03_infoMap() {
        return InfoMap.unweighted(g, pageRankResult::score, InfoMap.THRESHOLD, InfoMap.TAU, Pools.FJ_POOL, concurrency,ProgressLogger.NULL_LOGGER, TerminationFlag.RUNNING_TRUE)
                .compute()
                .getCommunityCount();
    }
}

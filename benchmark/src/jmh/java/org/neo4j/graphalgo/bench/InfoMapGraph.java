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
import org.neo4j.graphalgo.api.NodeWeights;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.helper.ldbc.LdbcDownloader;
import org.neo4j.graphalgo.impl.pagerank.PageRank;
import org.neo4j.graphalgo.impl.pagerank.PageRankFactory;
import org.neo4j.graphalgo.impl.results.CentralityResult;
import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.FormattedLog;
import org.neo4j.logging.Level;
import org.neo4j.logging.Log;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.stream.LongStream;

@State(Scope.Benchmark)
public class InfoMapGraph {

    private GraphDatabaseAPI db;
    Graph graph;
    ProgressLogger progressLogger;
    NodeWeights pageRanks;

    final int concurrency = 2;
    final double tau = 0.3;
    final int iterations = 15;
    final double threshold = 0.01;

    @Setup
    public void setup() throws IOException {
        db = LdbcDownloader.openDb("Yelp");

        Log log = FormattedLog.withLogLevel(Level.DEBUG).toOutputStream(System.out);
        AllocationTracker tracker = AllocationTracker.create();

        graph = new GraphLoader(db, Pools.DEFAULT)
                .withConcurrency(concurrency)
                .withAllocationTracker(tracker)
                .withLog(log)
                .withLogInterval(1L, TimeUnit.SECONDS)
                .asUndirected(true)
                .withAnyLabel()
                .withAnyRelationshipType()
                .load(HeavyGraphFactory.class);


        CentralityResult pr = PageRankFactory
                .of(
                        tracker,
                        graph,
                        new PageRank.Config(iterations, 1.0 - tau),
                        LongStream.empty(),
                        Pools.DEFAULT,
                        concurrency,
                        ParallelUtil.DEFAULT_BATCH_SIZE)
                .compute()
                .result();

        long[] allRels = new long[1];
        graph.forEachNode(node -> {
            allRels[0] += (long) (graph.degree(node, Direction.OUTGOING));
            return true;
        });

        log.info("Graph with %,d nodes and %,d relationships", graph.nodeCount(), allRels[0]);

        progressLogger = ProgressLogger.wrap(log, "InfoMap", 1L, TimeUnit.SECONDS);
        pageRanks = pr::score;
    }

    @TearDown
    public void shutdown() {
        graph.release();
        db.shutdown();
        Pools.DEFAULT.shutdownNow();
    }
}

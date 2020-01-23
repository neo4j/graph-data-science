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

package org.neo4j.graphalgo.bench;

import org.neo4j.graphalgo.StoreLoaderBuilder;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.compat.MapUtil;
import org.neo4j.graphalgo.core.loading.HugeGraphFactory;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.nodesim.ImmutableNodeSimilarityStreamConfig;
import org.neo4j.graphalgo.nodesim.NodeSimilarity;
import org.neo4j.graphalgo.nodesim.NodeSimilarityBaseConfig;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
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
import org.openjdk.jmh.infra.Blackhole;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Threads(1)
@Fork(value = 1, jvmArgs = {"-Xms4g", "-Xmx4g"})
@Warmup(iterations = 10, time = 1)
@Measurement(iterations = 10, time = 1)
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class NodeSimilarityBenchmark {

    private GraphDatabaseAPI db;
    private Graph graph;

    @Param(value = {"1", "4"})
    int concurrency;

    @Param(value = {"0", "100"})
    int topN;

    @Param(value = {"0", "100"})
    int topK;

    @Param(value = {"5"})
    int scaleFactor;

    private NodeSimilarityBaseConfig config;

    @Setup
    public void setup() {
        config = ImmutableNodeSimilarityStreamConfig
            .builder()
            .similarityCutoff(0)
            .degreeCutoff(0)
            .topN(topN)
            .topK(topK)
            .concurrency(concurrency)
            .direction(Direction.OUTGOING)
            .build();
        db = TestDatabaseCreator.createTestDatabase();

        createGraph(db, scaleFactor);

        this.graph = new StoreLoaderBuilder()
            .api(db)
            .loadAnyLabel()
            .loadAnyRelationshipType()
            .build()
            .graph(HugeGraphFactory.class);
    }

    @TearDown
    public void tearDown() {
        db.shutdown();
        Pools.DEFAULT.shutdownNow();
    }

    @Benchmark
    public void nodeSimilarityToStream(Blackhole blackhole) {
        initAlgo(config).computeToStream().forEach(blackhole::consume);
    }

    @Benchmark
    public void nodeSimilarityToGraph(Blackhole blackhole) {
        blackhole.consume(initAlgo(config).computeToGraph());
    }

    private NodeSimilarity initAlgo(NodeSimilarityBaseConfig config) {
        return new NodeSimilarity(
            graph,
            config,
            Pools.DEFAULT,
            AllocationTracker.EMPTY
        );
    }

    private List<Map<String, Object>> prepareProcedureInput() {
        List<Map<String, Object>> jaccardInput = new ArrayList<>();
        graph.forEachNode(nodeId -> {
            List<Number> targetIds = new ArrayList<>();
            graph.forEachRelationship(nodeId, Direction.OUTGOING, (sourceNodeId, targetNodeId) -> {
                targetIds.add(targetNodeId);
                return true;
            });
            if (!targetIds.isEmpty()) {
                jaccardInput.add(MapUtil.map("item", nodeId, "categories", targetIds));
            }
            return true;
        });
        return jaccardInput;
    }

    static void createGraph(GraphDatabaseService db, int scaleFactor) {
        int itemCount = 1_000 * scaleFactor;
        Label itemLabel = Label.label("Item");
        int personCount = 10_000 * scaleFactor;
        Label personLabel = Label.label("Person");
        RelationshipType likesType = RelationshipType.withName("LIKES");

        List<Node> itemNodes = new ArrayList<>();
        try (Transaction tx = db.beginTx()) {
            for (int i = 0; i < itemCount; i++) {
                itemNodes.add(db.createNode(itemLabel));
            }
            for (int i = 0; i < personCount; i++) {
                Node person = db.createNode(personLabel);
                if (i % 6 == 0) {
                    int itemIndex = Math.floorDiv(i, 15);
                    person.createRelationshipTo(itemNodes.get(itemIndex), likesType);
                    if (itemIndex > 0) person.createRelationshipTo(itemNodes.get(itemIndex - 1), likesType);
                }
                if (i % 5 == 0) {
                    int itemIndex = Math.floorDiv(i, 10);
                    person.createRelationshipTo(itemNodes.get(itemIndex), likesType);
                    if (itemIndex + 1 < itemCount) person.createRelationshipTo(itemNodes.get(itemIndex + 1), likesType);
                }
                if (i % 4 == 0) {
                    int itemIndex = Math.floorDiv(i, 20);
                    person.createRelationshipTo(itemNodes.get(itemIndex), likesType);
                    person.createRelationshipTo(itemNodes.get(itemIndex + 10), likesType);
                }
            }
            tx.success();
        }
    }
}

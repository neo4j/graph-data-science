/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
package org.neo4j.graphalgo.pagerank;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.AlgoTestBase;
import org.neo4j.graphalgo.CypherLoaderBuilder;
import org.neo4j.graphalgo.Orientation;
import org.neo4j.graphalgo.StoreLoaderBuilder;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphalgo.TestSupport.AllGraphTypesTest;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.GraphStoreFactory;
import org.neo4j.graphalgo.core.GraphDimensions;
import org.neo4j.graphalgo.core.ImmutableGraphDimensions;
import org.neo4j.graphalgo.core.ProcedureConfiguration;
import org.neo4j.graphalgo.core.loading.CypherFactory;
import org.neo4j.graphalgo.core.utils.BitUtil;
import org.neo4j.graphalgo.core.utils.mem.MemoryRange;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.results.CentralityResult;
import org.neo4j.graphdb.Label;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.graphalgo.QueryRunner.runInTransaction;
import static org.neo4j.graphalgo.compat.GraphDatabaseApiProxy.findNode;

final class PageRankTest extends AlgoTestBase {

    private static final Label LABEL = Label.label("Label1");
    private static final String RELATIONSHIP_TYPE = "TYPE1";

    private static PageRank.Config DEFAULT_CONFIG = new PageRank.Config(40, 0.85, PageRank.DEFAULT_TOLERANCE);

    private static final String DB_CYPHER =
            "CREATE" +
            "  (_:Label0 {name: '_'})" +
            ", (a:Label1 {name: 'a'})" +
            ", (b:Label1 {name: 'b'})" +
            ", (c:Label1 {name: 'c'})" +
            ", (d:Label1 {name: 'd'})" +
            ", (e:Label1 {name: 'e'})" +
            ", (f:Label1 {name: 'f'})" +
            ", (g:Label1 {name: 'g'})" +
            ", (h:Label1 {name: 'h'})" +
            ", (i:Label1 {name: 'i'})" +
            ", (j:Label1 {name: 'j'})" +
            ", (k:Label2 {name: 'k'})" +
            ", (l:Label2 {name: 'l'})" +
            ", (m:Label2 {name: 'm'})" +
            ", (n:Label2 {name: 'n'})" +
            ", (o:Label2 {name: 'o'})" +
            ", (p:Label2 {name: 'p'})" +
            ", (q:Label2 {name: 'q'})" +
            ", (r:Label2 {name: 'r'})" +
            ", (s:Label2 {name: 's'})" +
            ", (t:Label2 {name: 't'})" +
            ", (b)-[:TYPE1]->(c)" +
            ", (c)-[:TYPE1]->(b)" +
            ", (d)-[:TYPE1]->(a)" +
            ", (d)-[:TYPE1]->(b)" +
            ", (e)-[:TYPE1]->(b)" +
            ", (e)-[:TYPE1]->(d)" +
            ", (e)-[:TYPE1]->(f)" +
            ", (f)-[:TYPE1]->(b)" +
            ", (f)-[:TYPE1]->(e)" +
            ", (g)-[:TYPE2]->(b)" +
            ", (g)-[:TYPE2]->(e)" +
            ", (h)-[:TYPE2]->(b)" +
            ", (h)-[:TYPE2]->(e)" +
            ", (i)-[:TYPE2]->(b)" +
            ", (i)-[:TYPE2]->(e)" +
            ", (j)-[:TYPE2]->(e)" +
            ", (k)-[:TYPE2]->(e)";

    @BeforeEach
    void setupGraphDb() {
        db = TestDatabaseCreator.createTestDatabase();
        runQuery(DB_CYPHER);
    }

    @AfterEach
    void shutdownGraphDb() {
        db.shutdown();
    }

    @AllGraphTypesTest
    void testOnOutgoingRelationships(Class<? extends GraphStoreFactory> factoryType) {
        final Map<Long, Double> expected = new HashMap<>();

        runInTransaction(db, () -> {
            expected.put(findNode(db, LABEL, "name", "a").getId(), 0.243007);
            expected.put(findNode(db, LABEL, "name", "b").getId(), 1.9183995);
            expected.put(findNode(db, LABEL, "name", "c").getId(), 1.7806315);
            expected.put(findNode(db, LABEL, "name", "d").getId(), 0.21885);
            expected.put(findNode(db, LABEL, "name", "e").getId(), 0.243007);
            expected.put(findNode(db, LABEL, "name", "f").getId(), 0.21885);
            expected.put(findNode(db, LABEL, "name", "g").getId(), 0.15);
            expected.put(findNode(db, LABEL, "name", "h").getId(), 0.15);
            expected.put(findNode(db, LABEL, "name", "i").getId(), 0.15);
            expected.put(findNode(db, LABEL, "name", "j").getId(), 0.15);
        });

        final Graph graph;
        if (factoryType.isAssignableFrom(CypherFactory.class)) {
            graph = runInTransaction(db, () ->
                new CypherLoaderBuilder()
                    .api(db)
                    .nodeQuery(String.format("MATCH (n:%s) RETURN id(n) as id", LABEL.name()))
                    .relationshipQuery(String.format(
                        "MATCH (n:%s)-[:%s]->(m:%s) RETURN id(n) as source,id(m) as target",
                        LABEL.name(),
                        RELATIONSHIP_TYPE,
                        LABEL.name()
                    ))
                    .build()
                    .graph(factoryType)
            );
        } else {
            graph = new StoreLoaderBuilder()
                .api(db)
                .addNodeLabel(LABEL.name())
                .addRelationshipType(RELATIONSHIP_TYPE)
                .build()
                .graph(factoryType);
        }

        final CentralityResult rankResult = PageRankAlgorithmType.NON_WEIGHTED
                .create(graph, DEFAULT_CONFIG, LongStream.empty())
                .compute()
                .result();

        IntStream.range(0, expected.size()).forEach(i -> {
            final long nodeId = graph.toOriginalNodeId(i);
            assertEquals(
                    expected.get(nodeId),
                    rankResult.score(i),
                    1e-2,
                    "Node#" + nodeId
            );
        });
    }

    @AllGraphTypesTest
    void testOnIncomingRelationships(Class<? extends GraphStoreFactory> factoryType) {
        final Map<Long, Double> expected = new HashMap<>();

        runInTransaction(db, () -> {
            expected.put(findNode(db, LABEL, "name", "a").getId(), 0.15);
            expected.put(findNode(db, LABEL, "name", "b").getId(), 0.3386727);
            expected.put(findNode(db, LABEL, "name", "c").getId(), 0.2219679);
            expected.put(findNode(db, LABEL, "name", "d").getId(), 0.3494679);
            expected.put(findNode(db, LABEL, "name", "e").getId(), 2.5463981);
            expected.put(findNode(db, LABEL, "name", "f").getId(), 2.3858317);
            expected.put(findNode(db, LABEL, "name", "g").getId(), 0.15);
            expected.put(findNode(db, LABEL, "name", "h").getId(), 0.15);
            expected.put(findNode(db, LABEL, "name", "i").getId(), 0.15);
            expected.put(findNode(db, LABEL, "name", "j").getId(), 0.15);
        });

        final Graph graph;
        final CentralityResult rankResult;
        if (factoryType.isAssignableFrom(CypherFactory.class)) {
            graph = runInTransaction(db, () ->
                new CypherLoaderBuilder()
                    .api(db)
                    .nodeQuery(String.format("MATCH (n:%s) RETURN id(n) as id", LABEL.name()))
                    .relationshipQuery(String.format(
                        "MATCH (n:%s)<-[:%s]-(m:%s) RETURN id(n) as source,id(m) as target",
                        LABEL.name(),
                        RELATIONSHIP_TYPE,
                        LABEL.name()
                    ))
                    .build()
                    .graph(factoryType)
            );
            rankResult = PageRankAlgorithmType.NON_WEIGHTED
                    .create(graph, DEFAULT_CONFIG, LongStream.empty())
                    .compute()
                    .result();
        } else {
            graph = new StoreLoaderBuilder()
                .api(db)
                .addNodeLabel(LABEL.name())
                .addRelationshipType(RELATIONSHIP_TYPE)
                .globalOrientation(Orientation.REVERSE)
                .build()
                .graph(factoryType);

            rankResult = PageRankAlgorithmType.NON_WEIGHTED
                    .create(graph, DEFAULT_CONFIG, LongStream.empty())
                    .compute()
                    .result();
        }

        IntStream.range(0, expected.size()).forEach(i -> {
            final long nodeId = graph.toOriginalNodeId(i);
            assertEquals(
                    expected.get(nodeId),
                    rankResult.score(i),
                    1e-2,
                    "Node#" + nodeId
            );
        });
    }

    @AllGraphTypesTest
    void correctPartitionBoundariesForAllNodes(Class<? extends GraphStoreFactory> factoryType) {
        final Graph graph;
        if (factoryType.isAssignableFrom(CypherFactory.class)) {
            graph = runInTransaction(db, () -> new CypherLoaderBuilder()
                .api(db)
                .nodeQuery(String.format("MATCH (n:%s) RETURN id(n) as id", LABEL.name()))
                .relationshipQuery(String.format(
                    "MATCH (n:%s)-[:%s]->(m:%s) RETURN id(n) as source,id(m) as target",
                    LABEL.name(),
                    RELATIONSHIP_TYPE,
                    LABEL.name()
                ))
                .build()
                .graph(factoryType)
            );
        } else {
            graph = new StoreLoaderBuilder()
                .api(db)
                .addNodeLabel(LABEL.name())
                .addRelationshipType(RELATIONSHIP_TYPE)
                .build()
                .graph(factoryType);
        }

        // explicitly list all source nodes to prevent the 'we got everything' optimization
        PageRank algorithm = PageRankAlgorithmType.NON_WEIGHTED
            .create(
                graph,
                LongStream.range(0L, graph.nodeCount()),
                DEFAULT_CONFIG,
                1,
                null,
                1,
                AllocationTracker.EMPTY
            )
            .compute();
        // should not throw
    }

    @Test
    void shouldComputeMemoryEstimation1Thread() {
        long nodeCount = 100_000L;
        int concurrency = 1;
        assertMemoryEstimation(nodeCount, concurrency);
    }

    @Test
    void shouldComputeMemoryEstimation4Threads() {
        long nodeCount = 100_000L;
        int concurrency = 4;
        assertMemoryEstimation(nodeCount, concurrency);
    }

    @Test
    void shouldComputeMemoryEstimation42Threads() {
        long nodeCount = 100_000L;
        int concurrency = 42;
        assertMemoryEstimation(nodeCount, concurrency);
    }

    private void assertMemoryEstimation(final long nodeCount, final int concurrency) {
        GraphDimensions dimensions = ImmutableGraphDimensions.builder().nodeCount(nodeCount).build();

        final LabsPageRankFactory pageRank = new LabsPageRankFactory(DEFAULT_CONFIG);

        long partitionSize = BitUtil.ceilDiv(nodeCount, concurrency);
        final MemoryRange actual = pageRank
            .memoryEstimation(ProcedureConfiguration.empty())
            .estimate(dimensions, concurrency)
            .memoryUsage();
        final MemoryRange expected = MemoryRange.of(
                96L /* PageRank.class */ +
                32L /* ComputeSteps.class */ +
                BitUtil.align(16 + concurrency * 4, 8) /* scores[] wrapper */ +
                BitUtil.align(16 + concurrency * 8, 8) /* starts[] */ +
                BitUtil.align(16 + concurrency * 8, 8) /* lengths[] */ +
                BitUtil.align(16 + concurrency * 4, 8) /* list of computeSteps */ +
                        /* ComputeStep */
                concurrency * (
                        120L /* NonWeightedComputeStep.class */ +
                        BitUtil.align(16 + concurrency * 4, 8) /* nextScores[] wrapper */ +
                        concurrency * BitUtil.align(16 + partitionSize * 4, 8) /* inner nextScores[][] */ +
                        BitUtil.align(16 + partitionSize * 8, 8) /* pageRank[] */ +
                        BitUtil.align(16 + partitionSize * 8, 8) /* deltas[] */
                )
        );
        assertEquals(expected, actual);
    }
}

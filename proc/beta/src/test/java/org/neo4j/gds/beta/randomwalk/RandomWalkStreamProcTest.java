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
package org.neo4j.gds.beta.randomwalk;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.AlgoBaseProc;
import org.neo4j.gds.AlgoBaseProcTest;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.MemoryEstimateTest;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.SourceNodesConfigTest;
import org.neo4j.gds.TestSupport;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.core.utils.mem.MemoryRange;
import org.neo4j.gds.traversal.RandomWalk;
import org.neo4j.gds.traversal.RandomWalkStreamConfig;
import org.neo4j.graphdb.Path;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.gds.TestSupport.assertCypherMemoryEstimation;

@SuppressWarnings("unchecked")
class RandomWalkStreamProcTest extends BaseProcTest implements
    AlgoBaseProcTest<RandomWalk, RandomWalkStreamConfig, Stream<long[]>>,
    SourceNodesConfigTest<RandomWalk, RandomWalkStreamConfig, Stream<long[]>>,
    MemoryEstimateTest<RandomWalk, RandomWalkStreamConfig, Stream<long[]>> {

    private static final String DB_CYPHER =
        "CREATE" +
        "  (a:Node1)" +
        ", (b:Node1)" +
        ", (c:Node2)" +
        ", (d:Isolated)" +
        ", (e:Isolated)" +
        ", (a)-[:REL1]->(b)" +
        ", (b)-[:REL1]->(a)" +
        ", (a)-[:REL1]->(c)" +
        ", (c)-[:REL2]->(a)" +
        ", (b)-[:REL2]->(c)" +
        ", (c)-[:REL2]->(b)";

    @BeforeEach
    void setup() throws Exception {
        registerProcedures(RandomWalkStreamProc.class, GraphProjectProc.class);
        runQuery(DB_CYPHER);

        var createQuery = GdsCypher.call(DEFAULT_GRAPH_NAME)
            .graphProject()
            .withNodeLabels("Node1", "Node2")
            .loadEverything(Orientation.UNDIRECTED)
            .yields();
        runQuery(createQuery);
    }

    @Test
    void shouldRunSimpleConfig() {
        String query = GdsCypher.call(DEFAULT_GRAPH_NAME)
            .algo("gds", "beta", "randomWalk")
            .streamMode()
            .addParameter("walksPerNode", 3)
            .addParameter("walkLength", 10)
            .yields();

        Collection<List<Long>> result = new ArrayList<>();

        runQueryWithRowConsumer(query, row -> result.add((List<Long>) row.get("nodeIds")));

        int expectedNumberOfWalks = 3 * 3;
        assertEquals(expectedNumberOfWalks, result.size());
        List<Long> walkForNodeZero = result
            .stream()
            .filter(arr -> arr.get(0) == 0)
            .findFirst()
            .orElse(List.of());
        assertEquals(10, walkForNodeZero.size());
    }

    @Test
    void shouldReturnPath() {
        String query = GdsCypher.call(DEFAULT_GRAPH_NAME)
            .algo("gds", "beta", "randomWalk")
            .streamMode()
            .addParameter("walksPerNode", 3)
            .addParameter("walkLength", 10)
            .yields("nodeIds", "path");

        List<List<Long>> nodeIds = new ArrayList<>();
        List<Path> paths = new ArrayList<>();

        runQueryWithRowConsumer(query, row -> {
            nodeIds.add((List<Long>) row.get("nodeIds"));
            paths.add((Path) row.get("path"));
        });

        for (int i = 0; i < paths.size(); i++) {
            var nodes = nodeIds.get(i);
            var path = paths.get(i);

            AtomicInteger indexInPath = new AtomicInteger(0);
            path.nodes().forEach(node -> assertEquals(node.getId(), nodes.get(indexInPath.getAndIncrement())));
        }
    }

    @Test
    void shouldStopWhenStreamIsNotLongerConsumed() {
        var pool = (ThreadPoolExecutor) Pools.DEFAULT;
        assumeThat(pool.getActiveCount()).as("Test requires that no other threads are currently running").isEqualTo(0);

        // re-setup with a larger graph
        GraphStoreCatalog.removeAllLoadedGraphs();
        for (int i = 0; i < 10; i++) {
            runQuery(DB_CYPHER);
        }
        runQuery(GdsCypher.call(DEFAULT_GRAPH_NAME)
            .graphProject()
            .loadEverything(Orientation.UNDIRECTED)
            .yields());

        // concurrency must be > 1 to move the tasks to new threads
        // walkBufferSize must be small to get threads to block on flushBuffer steps
        var concurrency = 4;
        String query = GdsCypher.call(DEFAULT_GRAPH_NAME)
            .algo("gds", "beta", "randomWalk")
            .streamMode()
            .addParameter("walksPerNode", 10)
            .addParameter("walkLength", 10)
            .addParameter("walkBufferSize", 1)
            .addParameter("concurrency", concurrency)
            .yields("nodeIds", "path");

        // limit the result to 2 elements, terminating the stream after that
        query += " RETURN nodeIds, path LIMIT 2";

        runQueryWithResultConsumer(query, result -> {
            // we might not have started the procedure
            assertThat(pool.getActiveCount()).isBetween(0, concurrency);

            // we have the first result available
            assertThat(result).hasNext();

            // no useful assertion on the actual content
            assertThat(result.next()).isNotNull();

            // after the first result, we have at least one thread still running
            assertThat(pool.getActiveCount()).isBetween(1, concurrency);

            // we have one more result, but we will close the stream before consuming it
            assertThat(result).hasNext();
        });

        // after closing the result, the running threads should be interrupted
        // though it is not entirely deterministic when that will happen.
        // We'll loop for "5" seconds, and failing if the threads are still running after that
        // On CI, we wait a bit longer
        long timeoutInSeconds = 5 * (TestSupport.CI ? 5 : 1);
        var deadline = Instant.now().plus(timeoutInSeconds, ChronoUnit.SECONDS);

        while (Instant.now().isBefore(deadline)) {
            if (pool.getActiveCount() == 0) {
                break;
            }

            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(100));
        }

        // we're done or fail the test
        assertThat(pool.getActiveCount()).isEqualTo(0);
    }

    @Test
    void shouldThrowOnUnknownStartNode() {
        String query = GdsCypher.call(DEFAULT_GRAPH_NAME)
            .algo("gds", "beta", "randomWalk")
            .streamMode()
            .addParameter("walksPerNode", 3)
            .addParameter("walkLength", 10)
            .addParameter("sourceNodes", 42)
            .yields();

        assertError(query, "Source nodes do not exist in the in-memory graph: ['42']");
    }

    @Test
    void shouldThrowOnUnselectedStartNode() {
        String query = GdsCypher.call(DEFAULT_GRAPH_NAME)
            .algo("gds", "beta", "randomWalk")
            .streamMode()
            .addParameter("walksPerNode", 3)
            .addParameter("walkLength", 10)
            .addParameter("sourceNodes", 3)
            .addParameter("nodeLabels", List.of("Node1", "Node2"))
            .yields();

        assertError(query, "Source nodes do not exist in the in-memory graph for the labels ['Node1', 'Node2']: ['3']");
    }

    @Test
    void shouldRunMemoryEstimation() {
        String query = GdsCypher.call(DEFAULT_GRAPH_NAME)
            .algo("gds", "beta", "randomWalk")
            .estimationMode(GdsCypher.ExecutionModes.STREAM)
            .addParameter("walksPerNode", 3)
            .addParameter("walkLength", 10)
            .yields("bytesMin", "bytesMax", "nodeCount", "relationshipCount");

        assertCypherMemoryEstimation(db, query, MemoryRange.of(4_016, 100_032), 5, 12);
    }

    @Override
    public Class<? extends AlgoBaseProc<RandomWalk, Stream<long[]>, RandomWalkStreamConfig, ?>> getProcedureClazz() {
        return RandomWalkStreamProc.class;
    }

    @Override
    public GraphDatabaseAPI graphDb() {
        return db;
    }

    @Override
    public RandomWalkStreamConfig createConfig(CypherMapWrapper mapWrapper) {
        return RandomWalkStreamConfig.of(mapWrapper);
    }

    @Override
    public void assertResultEquals(Stream<long[]> result1, Stream<long[]> result2) {
        var resultList1 = result1.collect(Collectors.toList());
        var resultList2 = result2.collect(Collectors.toList());

        for (int i = 0; i < resultList1.size(); i++) {
            var path1 = resultList1.get(i);
            var path2 = resultList2.get(i);
            assertThat(path1.length).isEqualTo(path2.length);
        }
    }

    @Override
    public boolean releaseAlgorithm() {
        return false;
    }
}

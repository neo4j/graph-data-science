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

package org.neo4j.graphalgo.impl.jaccard;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.loading.HugeGraphFactory;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.NullLog;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.neo4j.graphdb.Direction.BOTH;
import static org.neo4j.graphdb.Direction.INCOMING;
import static org.neo4j.graphdb.Direction.OUTGOING;

final class NeighborhoodSimilarityTest {

    // TODO: maybe create random graph similar to JaccardProcTest#buildRandomDB
    private static final String DB_CYPHER =
        "CREATE" +
        "  (a:Person {name: 'Alice'})" +
        ", (b:Person {name: 'Bob'})" +
        ", (c:Person {name: 'Charlie'})" +
        ", (d:Person {name: 'Dave'})" +
        ", (i1:Item {name: 'p1'})" +
        ", (i2:Item {name: 'p2'})" +
        ", (i3:Item {name: 'p3'})" +
        ", (i4:Item {name: 'p4'})" +
        ", (a)-[:LIKES]->(i1)" +
        ", (a)-[:LIKES]->(i2)" +
        ", (a)-[:LIKES]->(i3)" +
        ", (b)-[:LIKES]->(i1)" +
        ", (b)-[:LIKES]->(i2)" +
        ", (c)-[:LIKES]->(i3)";

    private static final Collection<SimilarityResult> EXPECTED_OUTGOING = new HashSet<>();
    private static final Collection<SimilarityResult> EXPECTED_INCOMING = new HashSet<>();

    private static final Collection<SimilarityResult> EXPECTED_OUTGOING_TOP_1 = new HashSet<>();
    private static final Collection<SimilarityResult> EXPECTED_INCOMING_TOP_1 = new HashSet<>();

    private static final Collection<SimilarityResult> EXPECTED_OUTGOING_TOPK_1 = new HashSet<>();
    private static final Collection<SimilarityResult> EXPECTED_INCOMING_TOPK_1 = new HashSet<>();

    private static final Collection<SimilarityResult> EXPECTED_OUTGOING_SIMILARITY_CUTOFF = new HashSet<>();
    private static final Collection<SimilarityResult> EXPECTED_INCOMING_SIMILARITY_CUTOFF = new HashSet<>();

    private static final Collection<SimilarityResult> EXPECTED_OUTGOING_DEGREE_CUTOFF = new HashSet<>();
    private static final Collection<SimilarityResult> EXPECTED_INCOMING_DEGREE_CUTOFF = new HashSet<>();

    static {
        EXPECTED_OUTGOING.add(new SimilarityResult(0, 1, 2 / 3.0));
        EXPECTED_OUTGOING.add(new SimilarityResult(0, 2, 1 / 3.0));
        EXPECTED_OUTGOING.add(new SimilarityResult(1, 2, 0.0));

        EXPECTED_OUTGOING_TOP_1.add(new SimilarityResult(0, 1, 2 / 3.0));

        EXPECTED_OUTGOING_TOPK_1.add(new SimilarityResult(0, 1, 2 / 3.0));
        EXPECTED_OUTGOING_TOPK_1.add(new SimilarityResult(1, 0, 2 / 3.0));
        EXPECTED_OUTGOING_TOPK_1.add(new SimilarityResult(2, 0, 1 / 3.0));

        EXPECTED_OUTGOING_SIMILARITY_CUTOFF.add(new SimilarityResult(0, 1, 2 / 3.0));
        EXPECTED_OUTGOING_SIMILARITY_CUTOFF.add(new SimilarityResult(0, 2, 1 / 3.0));

        EXPECTED_OUTGOING_DEGREE_CUTOFF.add(new SimilarityResult(0, 1, 2 / 3.0));
        EXPECTED_OUTGOING_DEGREE_CUTOFF.add(new SimilarityResult(0, 2, 1 / 3.0));
        EXPECTED_OUTGOING_DEGREE_CUTOFF.add(new SimilarityResult(1, 2, 0.0));

        EXPECTED_INCOMING.add(new SimilarityResult(4, 5, 3.0 / 3.0));
        EXPECTED_INCOMING.add(new SimilarityResult(4, 6, 1 / 3.0));
        EXPECTED_INCOMING.add(new SimilarityResult(5, 6, 1 / 3.0));

        EXPECTED_INCOMING_TOP_1.add(new SimilarityResult(4, 5, 3.0 / 3.0));

        EXPECTED_INCOMING_TOPK_1.add(new SimilarityResult(4, 5, 3.0 / 3.0));
        EXPECTED_INCOMING_TOPK_1.add(new SimilarityResult(5, 4, 3.0 / 3.0));
        EXPECTED_INCOMING_TOPK_1.add(new SimilarityResult(6, 4, 1 / 3.0));

        EXPECTED_INCOMING_SIMILARITY_CUTOFF.add(new SimilarityResult(4, 5, 3.0 / 3.0));
        EXPECTED_INCOMING_SIMILARITY_CUTOFF.add(new SimilarityResult(4, 6, 1 / 3.0));
        EXPECTED_INCOMING_SIMILARITY_CUTOFF.add(new SimilarityResult(5, 6, 1 / 3.0));

        EXPECTED_INCOMING_DEGREE_CUTOFF.add(new SimilarityResult(4, 5, 3.0 / 3.0));
        EXPECTED_INCOMING_DEGREE_CUTOFF.add(new SimilarityResult(4, 6, 1 / 3.0));
        EXPECTED_INCOMING_DEGREE_CUTOFF.add(new SimilarityResult(5, 6, 1 / 3.0));
    }

    private GraphDatabaseAPI db;

    @BeforeEach
    void setup() {
        db = TestDatabaseCreator.createTestDatabase();
        db.execute(DB_CYPHER);
    }

    @AfterEach
    void teardown() {
        db.shutdown();
    }

    @ParameterizedTest
    @CsvSource({
        "OUTGOING, OUTGOING",
        "BOTH, OUTGOING",
        "INCOMING, INCOMING",
        "BOTH, INCOMING"
    })
    void shouldComputeForSupportedDirections(Direction loadDirection, Direction algoDirection) {
        Graph graph = new GraphLoader(db)
            .withAnyLabel()
            .withAnyRelationshipType()
            .withDirection(loadDirection)
            .load(HugeGraphFactory.class);

        NeighborhoodSimilarity neighborhoodSimilarity = new NeighborhoodSimilarity(
            graph,
            NeighborhoodSimilarity.Config.DEFAULT,
            Pools.DEFAULT,
            AllocationTracker.EMPTY,
            NullLog.getInstance()
        );

        Set<SimilarityResult> result = neighborhoodSimilarity.run(algoDirection).collect(Collectors.toSet());
        neighborhoodSimilarity.release();

        assertEquals(algoDirection == INCOMING ? EXPECTED_INCOMING : EXPECTED_OUTGOING, result);
    }

    @ParameterizedTest
    @CsvSource({
        "OUTGOING, OUTGOING",
        "BOTH, OUTGOING",
        "INCOMING, INCOMING",
        "BOTH, INCOMING"
    })
    void shouldComputeTopForSupportedDirections(Direction loadDirection, Direction algoDirection) {
        Graph graph = new GraphLoader(db)
            .withAnyLabel()
            .withAnyRelationshipType()
            .withDirection(loadDirection)
            .load(HugeGraphFactory.class);

        NeighborhoodSimilarity neighborhoodSimilarity = new NeighborhoodSimilarity(
            graph,
            new NeighborhoodSimilarity.Config(0.0, 0, 1, 0, Pools.DEFAULT_CONCURRENCY, ParallelUtil.DEFAULT_BATCH_SIZE),
            Pools.DEFAULT,
            AllocationTracker.EMPTY,
            NullLog.getInstance());

        Set<SimilarityResult> result = neighborhoodSimilarity.run(algoDirection).collect(Collectors.toSet());
        neighborhoodSimilarity.release();

        assertEquals(algoDirection == INCOMING ? EXPECTED_INCOMING_TOP_1 : EXPECTED_OUTGOING_TOP_1, result);
    }

    @ParameterizedTest
    @CsvSource({
        "OUTGOING, OUTGOING",
        "BOTH, OUTGOING",
        "INCOMING, INCOMING",
        "BOTH, INCOMING"
    })
    void shouldComputeTopKForSupportedDirections(Direction loadDirection, Direction algoDirection) {
        Graph graph = new GraphLoader(db)
            .withAnyLabel()
            .withAnyRelationshipType()
            .withDirection(loadDirection)
            .load(HugeGraphFactory.class);

        NeighborhoodSimilarity neighborhoodSimilarity = new NeighborhoodSimilarity(
            graph,
            new NeighborhoodSimilarity.Config(0.0, 0, 0, 1, Pools.DEFAULT_CONCURRENCY, ParallelUtil.DEFAULT_BATCH_SIZE),
            Pools.DEFAULT,
            AllocationTracker.EMPTY,
            NullLog.getInstance());

        Set<SimilarityResult> result = neighborhoodSimilarity.run(algoDirection).collect(Collectors.toSet());
        neighborhoodSimilarity.release();

        assertEquals(algoDirection == INCOMING ? EXPECTED_INCOMING_TOPK_1 : EXPECTED_OUTGOING_TOPK_1, result);
    }

    @ParameterizedTest
    @CsvSource({
        "OUTGOING, OUTGOING",
        "BOTH, OUTGOING",
        "INCOMING, INCOMING",
        "BOTH, INCOMING"
    })
    void shouldComputeWithSimilarityCutoffForSupportedDirections(Direction loadDirection, Direction algoDirection) {
        Graph graph = new GraphLoader(db)
            .withAnyLabel()
            .withAnyRelationshipType()
            .withDirection(loadDirection)
            .load(HugeGraphFactory.class);

        NeighborhoodSimilarity neighborhoodSimilarity = new NeighborhoodSimilarity(
            graph,
            new NeighborhoodSimilarity.Config(0.1, 0, 0, 0, Pools.DEFAULT_CONCURRENCY, ParallelUtil.DEFAULT_BATCH_SIZE),
            Pools.DEFAULT,
            AllocationTracker.EMPTY,
            NullLog.getInstance());

        Set<SimilarityResult> result = neighborhoodSimilarity.run(algoDirection).collect(Collectors.toSet());
        neighborhoodSimilarity.release();

        assertEquals(algoDirection == INCOMING ? EXPECTED_INCOMING_SIMILARITY_CUTOFF : EXPECTED_OUTGOING_SIMILARITY_CUTOFF, result);
    }

    @ParameterizedTest
    @CsvSource({
        "OUTGOING, OUTGOING",
        "BOTH, OUTGOING",
        "INCOMING, INCOMING",
        "BOTH, INCOMING"
    })
    void shouldComputeWithDegreeCutoffForSupportedDirections(Direction loadDirection, Direction algoDirection) {
        Graph graph = new GraphLoader(db)
            .withAnyLabel()
            .withAnyRelationshipType()
            .withDirection(loadDirection)
            .load(HugeGraphFactory.class);

        NeighborhoodSimilarity neighborhoodSimilarity = new NeighborhoodSimilarity(
            graph,
            new NeighborhoodSimilarity.Config(0.0, 1, 0, 0, Pools.DEFAULT_CONCURRENCY, ParallelUtil.DEFAULT_BATCH_SIZE),
            Pools.DEFAULT,
            AllocationTracker.EMPTY,
            NullLog.getInstance());

        Set<SimilarityResult> result = neighborhoodSimilarity.run(algoDirection).collect(Collectors.toSet());
        neighborhoodSimilarity.release();

        assertEquals(algoDirection == INCOMING ? EXPECTED_INCOMING_DEGREE_CUTOFF : EXPECTED_OUTGOING_DEGREE_CUTOFF, result);
    }

    @Test
    void shouldComputeForUndirectedGraphs() {
        Graph graph = new GraphLoader(db)
            .withAnyLabel()
            .withAnyRelationshipType()
            .undirected()
            .load(HugeGraphFactory.class);

        NeighborhoodSimilarity neighborhoodSimilarity = new NeighborhoodSimilarity(
            graph,
            NeighborhoodSimilarity.Config.DEFAULT,
            Pools.DEFAULT,
            AllocationTracker.EMPTY,
            NullLog.getInstance());
        Set<SimilarityResult> result = neighborhoodSimilarity.run(OUTGOING).collect(Collectors.toSet());
        neighborhoodSimilarity.release();
        assertNotEquals(Collections.emptySet(), result);
    }

    @Test
    void shouldThrowForDirectionBoth() {
        Graph graph = new GraphLoader(db)
            .withAnyLabel()
            .withAnyRelationshipType()
            .undirected()
            .load(HugeGraphFactory.class);

        IllegalArgumentException ex = Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> new NeighborhoodSimilarity(
                graph,
                NeighborhoodSimilarity.Config.DEFAULT,
                Pools.DEFAULT,
                AllocationTracker.EMPTY,
                NullLog.getInstance()).run(BOTH)
        );
        assertThat(ex.getMessage(), containsString("Direction BOTH is not supported"));
    }
}

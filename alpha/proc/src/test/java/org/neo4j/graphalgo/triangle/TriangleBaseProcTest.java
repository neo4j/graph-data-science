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
package org.neo4j.graphalgo.triangle;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.Algorithm;
import org.neo4j.graphalgo.BaseProcTest;
import org.neo4j.graphalgo.ElementIdentifier;
import org.neo4j.graphalgo.NodeProjections;
import org.neo4j.graphalgo.Projection;
import org.neo4j.graphalgo.RelationshipProjection;
import org.neo4j.graphalgo.RelationshipProjections;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.core.DeduplicationStrategy;
import org.neo4j.graphalgo.impl.triangle.TriangleConfig;
import org.neo4j.graphalgo.newapi.GraphCreateFromStoreConfig;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertThrows;

abstract class TriangleBaseProcTest<A extends Algorithm<A, RESULT>, RESULT, CONFIG extends TriangleConfig> extends BaseProcTest {
    /**
     *      (a)-- (b)--(d)--(e)
     *        \T1/       \T2/
     *        (c)   (g)  (f)
     *          \  /T3\
     *          (h)--(i)
     */
    String dbCypher() {
        return
            "CREATE " +
            "  (a:Node {name: 'a'})" +
            ", (b:Node {name: 'b'})" +
            ", (c:Node {name: 'c'})" +
            ", (d:Node {name: 'd'})" +
            ", (e:Node {name: 'e'})" +
            ", (f:Node {name: 'f'})" +
            ", (g:Node {name: 'g'})" +
            ", (h:Node {name: 'h'})" +
            ", (i:Node {name: 'i'})" +
            ", (a)-[:TYPE]->(b)" +
            ", (b)-[:TYPE]->(c)" +
            ", (c)-[:TYPE]->(a)" +
            ", (c)-[:TYPE]->(h)" +
            ", (d)-[:TYPE]->(e)" +
            ", (e)-[:TYPE]->(f)" +
            ", (f)-[:TYPE]->(d)" +
            ", (b)-[:TYPE]->(d)" +
            ", (g)-[:TYPE]->(h)" +
            ", (h)-[:TYPE]->(i)" +
            ", (i)-[:TYPE]->(g)";
    }

    @BeforeEach
    void setup() throws Exception {
        db = TestDatabaseCreator.createTestDatabase();
        registerProcedures(TriangleProc.class, TriangleCountProc.class, BalancedTriadsProc.class);
        runQuery(dbCypher());
    }

    @AfterEach
    void shutdownGraph() {
        db.shutdown();
    }

    abstract TriangleBaseProc<A, RESULT, CONFIG> newInstance();

    abstract CONFIG newConfig();

    @Test
    void testValidateUndirectedProjection() {
        RelationshipProjections invalidRelationshipProjections = RelationshipProjections.builder()
            .putProjection(
                ElementIdentifier.of("TYPE"),
                RelationshipProjection.of("TYPE", Projection.NATURAL, DeduplicationStrategy.DEFAULT)
            )
            .build();

        GraphCreateFromStoreConfig graphCreateFromStoreConfig = GraphCreateFromStoreConfig.of(
            getUsername(),
            "",
            NodeProjections.empty(),
            invalidRelationshipProjections,
            CypherMapWrapper.empty()
        );

        TriangleBaseProc<A, RESULT, CONFIG> proc = newInstance();

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> proc.validateGraphCreateConfig(graphCreateFromStoreConfig, newConfig()));

        assertThat(ex.getMessage(), containsString("Projection for `TYPE` uses projection `NATURAL`"));
    }
}

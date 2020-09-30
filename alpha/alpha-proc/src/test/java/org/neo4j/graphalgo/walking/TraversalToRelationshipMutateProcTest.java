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
package org.neo4j.graphalgo.walking;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.AlgoBaseProc;
import org.neo4j.graphalgo.AlgoBaseProcTest;
import org.neo4j.graphalgo.BaseProcTest;
import org.neo4j.graphalgo.GdsCypher;
import org.neo4j.graphalgo.MutateRelationshipsTest;
import org.neo4j.graphalgo.RelationshipType;
import org.neo4j.graphalgo.api.Relationships;
import org.neo4j.graphalgo.catalog.GraphCreateProc;
import org.neo4j.graphalgo.catalog.GraphWriteNodePropertiesProc;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.impl.walking.TraversalToRelationship;
import org.neo4j.graphalgo.impl.walking.TraversalToRelationshipConfig;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.List;
import java.util.Optional;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.jupiter.api.Assertions.assertEquals;

class TraversalToRelationshipMutateProcTest extends BaseProcTest implements
    AlgoBaseProcTest<TraversalToRelationship, TraversalToRelationshipConfig, Relationships>,
    MutateRelationshipsTest<TraversalToRelationship, TraversalToRelationshipConfig, Relationships> {

    @Override
    public String createQuery() {
        return "CREATE" +
               "  (a:Person)" +
               ", (b:Person)" +
               ", (c:Person)" +

               ", (a)-[:KNOWS]->(b)" +
               ", (b)-[:KNOWS]->(c)";
    }

    @Override
    public GraphDatabaseAPI graphDb() {
        return db;
    }

    @BeforeEach
    void setupGraph() throws Exception {
        registerProcedures(
            TraversalToRelationshipMutateProc.class,
            GraphCreateProc.class,
            GraphWriteNodePropertiesProc.class
        );

        runQuery(createQuery());
    }

    @Override
    public String mutateRelationshipType() {
        return "FoF";
    }

    @Override
    public String expectedMutatedGraph() {
        return "CREATE" +
               "  (a)" +
               ", (b)" +
               ", (c)" +

               ", (a)-[:KNOWS]->(b)" +
               ", (b)-[:KNOWS]->(c)" +
               ", (a)-[:FoF]->(c)";
    }

    @Override
    public Class<? extends AlgoBaseProc<TraversalToRelationship, Relationships, TraversalToRelationshipConfig>> getProcedureClazz() {
        return TraversalToRelationshipMutateProc.class;
    }

    @Override
    public CypherMapWrapper createMinimalConfig(CypherMapWrapper mapWrapper) {
        mapWrapper = MutateRelationshipsTest.super.createMinimalConfig(mapWrapper);

        if (!mapWrapper.containsKey("relationshipTypes")) {
            mapWrapper = mapWrapper.withEntry(
                "relationshipTypes",
                List.of(RelationshipType.ALL_RELATIONSHIPS.name(), RelationshipType.ALL_RELATIONSHIPS.name())
            );
        }

        return mapWrapper;
    }

    @Override
    public TraversalToRelationshipConfig createConfig(CypherMapWrapper mapWrapper) {
        return TraversalToRelationshipConfig.of(getUsername(), Optional.empty(), Optional.empty(), mapWrapper);
    }

    @Override
    public void assertResultEquals(Relationships result1, Relationships result2) {
        assertEquals(result1.topology().orientation(), result2.topology().orientation());
        assertEquals(result1.topology().elementCount(), result2.topology().elementCount());
        assertEquals(result1.topology().isMultiGraph(), result2.topology().isMultiGraph());

        assertEquals(result1.properties().isPresent(), result2.properties().isPresent());
    }

    @Test
    void testMutateYields() {
        String query = GdsCypher
            .call()
            .withAnyLabel()
            .withRelationshipType("KNOWS")
            .algo("gds.alpha.traversalToRelationship")
            .mutateMode()
            .addParameter("relationshipTypes", List.of("KNOWS", "KNOWS"))
            .addParameter("mutateRelationshipType", "FoF")
            .yields(
                "createMillis",
                "computeMillis",
                "mutateMillis",
                "relationshipsWritten",
                "configuration"
            );

        runQueryWithRowConsumer(
            query,
            row -> {
                assertEquals(1L, row.getNumber("relationshipsWritten"));

                assertThat(-1L, lessThan(row.getNumber("createMillis").longValue()));
                assertThat(-1L, lessThan(row.getNumber("computeMillis").longValue()));
                assertThat(-1L, lessThan(row.getNumber("mutateMillis").longValue()));
            }
        );
    }
}

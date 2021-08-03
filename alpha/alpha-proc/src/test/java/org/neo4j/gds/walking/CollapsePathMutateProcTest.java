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
package org.neo4j.gds.walking;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.AlgoBaseProc;
import org.neo4j.gds.AlgoBaseProcTest;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.MutateRelationshipsTest;
import org.neo4j.gds.catalog.GraphCreateProc;
import org.neo4j.gds.catalog.GraphWriteNodePropertiesProc;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.extension.Neo4jGraph;
import org.neo4j.gds.impl.walking.CollapsePath;
import org.neo4j.gds.impl.walking.CollapsePathConfig;
import org.neo4j.graphalgo.RelationshipType;
import org.neo4j.graphalgo.api.Relationships;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertEquals;

class CollapsePathMutateProcTest extends BaseProcTest implements
    AlgoBaseProcTest<CollapsePath, CollapsePathConfig, Relationships>,
    MutateRelationshipsTest<CollapsePath, CollapsePathConfig, Relationships> {

    @Neo4jGraph
    public static final String DB_CYPHER = "CREATE" +
           "  (a:Person)" +
           ", (b:Person)" +
           ", (c:Person)" +

           ", (a)-[:KNOWS]->(b)" +
           ", (b)-[:KNOWS]->(c)";

    @Override
    public GraphDatabaseAPI graphDb() {
        return db;
    }

    @BeforeEach
    void setupGraph() throws Exception {
        registerProcedures(
            getProcedureClazz(),
            GraphCreateProc.class,
            GraphWriteNodePropertiesProc.class
        );
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
    public Class<? extends AlgoBaseProc<CollapsePath, Relationships, CollapsePathConfig>> getProcedureClazz() {
        return CollapsePathMutateProc.class;
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
    public CollapsePathConfig createConfig(CypherMapWrapper mapWrapper) {
        return CollapsePathConfig.of(getUsername(), Optional.empty(), Optional.empty(), mapWrapper);
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
        String graphName = "graph";
        String loadQuery = GdsCypher.call()
            .withAnyLabel()
            .withRelationshipType("KNOWS")
            .graphCreate(graphName)
            .yields();

        runQuery(loadQuery);

        String query = GdsCypher
            .call()
            .explicitCreation(graphName)
            .algo("gds.alpha.collapsePath")
            .mutateMode()
            .addParameter("relationshipTypes", List.of("KNOWS", "KNOWS"))
            .addParameter("mutateRelationshipType", "FoF")
            .yields();

        assertCypherResult(
            query,
            List.of(
                Map.of(
                    "relationshipsWritten", 1L,
                    "createMillis", greaterThan(-1L),
                    "computeMillis", greaterThan(-1L),
                    "mutateMillis", greaterThan(-1L),
                    "configuration", instanceOf(Map.class)
                )
            )
        );
    }
}

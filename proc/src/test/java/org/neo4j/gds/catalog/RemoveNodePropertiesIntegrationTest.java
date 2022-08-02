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
package org.neo4j.gds.catalog;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.TestSupport;
import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.wcc.WccMutateProc;

import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.neo4j.gds.compat.MapUtil.map;

public class RemoveNodePropertiesIntegrationTest extends BaseProcTest {
    private static final String DB_CYPHER =
        "CREATE" +
        "  (a:Node)" +
        ", (b:Node)" +
        ", (c:Node)" +
        ", (d:Node)" +
        ", (a)-[:TYPE]->(b)" +
        ", (b)-[:TYPE]->(c)" +
        ", (c)-[:TYPE]->(d)";

    @BeforeEach
    void setup() throws Exception {
        registerProcedures(GraphProjectProc.class, WccMutateProc.class, GraphDropNodePropertiesProc.class);
        runQuery(DB_CYPHER);

        runQuery(GdsCypher
            .call("testGraph")
            .graphProject()
            .withAnyLabel()
            .withRelationshipType("TYPE")
            .yields());
    }

    @AfterEach
    void shutdown() {
        GraphStoreCatalog.removeAllLoadedGraphs();
    }

    @Test
    void shouldBeAbleToMutateAndDelete() {
        Graph graphBefore = GraphStoreCatalog.get(getUsername(), DatabaseId.of(db), "testGraph").graphStore().getUnion();

        runQuery("CALL gds.wcc.mutate('testGraph', {mutateProperty: 'componentId'})");
        Graph graphAfterMutate = GraphStoreCatalog.get(getUsername(), DatabaseId.of(db), "testGraph").graphStore().getUnion();
        assertNotEquals(graphBefore.availableNodeProperties(), graphAfterMutate.availableNodeProperties());

        assertCypherResult(
            "CALL gds.graph.nodeProperties.drop('testGraph', ['componentId']) YIELD propertiesRemoved",
            singletonList(map("propertiesRemoved", 4L))
        );
        Graph graphAfterDelete = GraphStoreCatalog.get(getUsername(), DatabaseId.of(db), "testGraph").graphStore().getUnion();
        TestSupport.assertGraphEquals(graphBefore, graphAfterDelete);
    }
}

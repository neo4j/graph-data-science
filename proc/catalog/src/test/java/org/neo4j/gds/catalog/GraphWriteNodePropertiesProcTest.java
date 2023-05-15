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
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.core.utils.warnings.GlobalUserLogStore;
import org.neo4j.gds.core.utils.warnings.UserLogEntry;
import org.neo4j.gds.core.utils.warnings.UserLogRegistryExtension;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.ExtensionCallback;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class GraphWriteNodePropertiesProcTest extends BaseProcTest {

    private static final String TEST_GRAPH_SAME_PROPERTIES = "testGraph";

    private static final String DB_CYPHER =
        "CREATE" +
        "  (a:A {nodeProp1: 0, nodeProp2: 42})" +
        ", (b:A {nodeProp1: 1, nodeProp2: 43})" +
        ", (c:A {nodeProp1: 2, nodeProp2: 44})" +
        ", (d:B {nodeProp1: 3, nodeProp2: 45})" +
        ", (e:B {nodeProp1: 4, nodeProp2: 46})" +
        ", (f:B {nodeProp1: 5, nodeProp2: 47})";

    GlobalUserLogStore userLogStore;

    @Override
    @ExtensionCallback
    protected void configuration(TestDatabaseManagementServiceBuilder builder) {
        super.configuration(builder);
        this.userLogStore = new GlobalUserLogStore();
        builder.removeExtensions(extension -> extension instanceof UserLogRegistryExtension);
        builder.addExtension(new UserLogRegistryExtension(() -> userLogStore));
    }

    @BeforeEach
    void setup() throws Exception {
        registerProcedures(
            GraphProjectProc.class,
            GraphWriteNodePropertiesProc.class
        );
        runQuery(DB_CYPHER);

        runQuery(GdsCypher.call(TEST_GRAPH_SAME_PROPERTIES)
            .graphProject()
            .withNodeLabels("A", "B")
            .withNodeProperty("newNodeProp1", "nodeProp1")
            .withNodeProperty("newNodeProp2", "nodeProp2")
            .withAnyRelationshipType()
            .yields()
        );
    }

    @AfterEach
    void tearDown() {
        GraphStoreCatalog.removeAllLoadedGraphs();
    }


    @Test
    void shouldLogDeprecationWarning() {
        runQuery(
            "CALL gds.graph.writeNodeProperties($graph, ['newNodeProp1', 'newNodeProp2'])",
            Map.of("graph", TEST_GRAPH_SAME_PROPERTIES)
        );

        assertThat(userLogStore.query(getUsername()))
            .hasSize(1)
            .first()
            .extracting(UserLogEntry::getMessage)
            .asString()
            .contains("deprecated")
            .contains("gds.graph.nodeProperties.write");
    }
}

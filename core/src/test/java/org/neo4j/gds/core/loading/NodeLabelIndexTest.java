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
package org.neo4j.gds.core.loading;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseTest;
import org.neo4j.gds.StoreLoaderBuilder;
import org.neo4j.gds.compat.Neo4jProxy;
import org.neo4j.gds.compat.TestLog;
import org.neo4j.gds.extension.Neo4jGraph;
import org.neo4j.gds.extension.Neo4jGraphExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.gds.TestSupport.assertGraphEquals;
import static org.neo4j.gds.TestSupport.fromGdl;

@Neo4jGraphExtension
public class NodeLabelIndexTest extends BaseTest {

    @Neo4jGraph
    public static final String DB_CYPHER = "CREATE (a:Foo),(b:Bar)";

    @Test
    void shouldLoadWithoutNodeLabelIndex() {
        runQueryWithResultConsumer(
            "SHOW INDEXES WHERE entityType = 'NODE'",
            result -> {
                assertThat(result.hasNext()).isTrue();
                runQueryWithResultConsumer(
                    "DROP INDEX " + result.next().get("name"),
                    innerResult -> assertThat(innerResult.resultAsString()).contains("Indexes removed: 1")
                );
            }
        );

        var log = Neo4jProxy.testLog();;
        var graph = new StoreLoaderBuilder()
            .databaseService(db)
            .log(log)
            .addNodeLabel("Foo")
            .addNodeLabel("Bar")
            .build()
            .graph();

        assertGraphEquals(fromGdl("(:Foo),(:Bar)"), graph);

        assertThat(log.getMessages(TestLog.INFO))
            .contains("[gds] Attempted to use node label index, but no index was found. Falling back to node store scan.");
    }
}

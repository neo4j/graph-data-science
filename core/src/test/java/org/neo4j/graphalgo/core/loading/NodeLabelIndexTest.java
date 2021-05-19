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
package org.neo4j.graphalgo.core.loading;

import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.BaseTest;
import org.neo4j.graphalgo.StoreLoaderBuilder;
import org.neo4j.graphalgo.extension.Neo4jGraph;
import org.neo4j.graphalgo.extension.Neo4jGraphExtension;

import static org.neo4j.graphalgo.TestSupport.assertGraphEquals;
import static org.neo4j.graphalgo.TestSupport.fromGdl;

@Neo4jGraphExtension
public class NodeLabelIndexTest extends BaseTest {

    @Neo4jGraph
    public static final String DB_CYPHER = "CREATE (a:Foo)";

    @Test
    void shouldLoadWithoutNodeLabelIndex() {
        runQueryWithRowConsumer("SHOW INDEXES WHERE entityType = 'NODE'", row -> runQuery("DROP INDEX " + row.getString("name")));

        var graph = new StoreLoaderBuilder()
            .api(db)
            .addNodeLabel("Foo")
            .build()
            .graph();

        assertGraphEquals(fromGdl("(:Foo)"), graph);
    }
}

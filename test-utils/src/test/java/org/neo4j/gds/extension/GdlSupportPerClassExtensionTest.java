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
package org.neo4j.gds.extension;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.config.GraphProjectFromStoreConfig;
import org.neo4j.gds.core.loading.GraphStoreCatalog;

import static org.assertj.core.api.Assertions.assertThat;

@GdlPerClassExtension
class GdlSupportPerClassExtensionTest {

    @GdlGraph
    public static final String GRAPH = "(a)-[:REL]->(b)";

    @Inject
    GraphStore graphStore;

    @BeforeAll
    void setUp() {
        var graphProjectConfig = GraphProjectFromStoreConfig.emptyWithName("", "graph");
        GraphStoreCatalog.set(graphProjectConfig, graphStore);
    }

    @Test
    void testHasGraph() {
        assertThat(GraphStoreCatalog.get("", BaseGdlSupportExtension.DATABASE_ID, "graph")).isNotNull();
    }

    @Test
    void testStillHasGraph() {
        assertThat(GraphStoreCatalog.get("", BaseGdlSupportExtension.DATABASE_ID, "graph")).isNotNull();
    }
}

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
import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.properties.graph.LongGraphPropertyValues;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.extension.Neo4jGraph;

import java.util.List;
import java.util.Map;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

class GraphRemoveGraphPropertiesProcTest extends BaseProcTest {

    @Neo4jGraph
    private static final String GRAPH = "CREATE (a)";

    private GraphStore graphStore;


    @BeforeEach
    void setup() throws Exception {
        registerProcedures(GraphProjectProc.class, GraphRemoveGraphPropertiesProc.class);

        runQuery(GdsCypher.call(DEFAULT_GRAPH_NAME).graphProject().withAnyLabel().withAnyRelationshipType().yields());

        graphStore = GraphStoreCatalog.get("", DatabaseId.of(db), DEFAULT_GRAPH_NAME).graphStore();
    }

    @AfterEach
    void tearDown() {
        GraphStoreCatalog.removeAllLoadedGraphs();
    }

    @Test
    void removeAGraphProperty() {
        LongGraphPropertyValues values = new LongGraphPropertyValues() {
            @Override
            public LongStream longValues() {
                return LongStream.range(0, 10);
            }

            @Override
            public long size() {
                return 10;

            }
        };

        graphStore.addGraphProperty("prop", values);

        String graphWriteQuery = formatWithLocale(
            "CALL gds.alpha.graph.graphProperty.drop('%s', 'prop')",
            DEFAULT_GRAPH_NAME
        );

        assertCypherResult(
            graphWriteQuery,
            List.of(
                Map.of(
                  "graphName", DEFAULT_GRAPH_NAME,
                  "graphProperty", "prop",
                  "propertiesRemoved", 10L
                )
            )
        );

        assertThat(graphStore.hasGraphProperty("prop")).isFalse();
    }

    @Test
    void shouldFailOnNonExistingNodeProperty() {
        assertError(
            "CALL gds.alpha.graph.graphProperty.drop($graph, 'UNKNOWN')",
            Map.of("graph", DEFAULT_GRAPH_NAME),
            "The specified graph property 'UNKNOWN' does not exist. The following properties exist in the graph []."
        );
    }

    @Test
    void shouldFailOnNonExistingNodePropertyForSpecificLabel() {
        LongGraphPropertyValues values = new LongGraphPropertyValues() {
            @Override
            public LongStream longValues() {
                return LongStream.range(0, 10);
            }

            @Override
            public long size() {
                return 10;

            }
        };
        graphStore.addGraphProperty("prop", values);
        assertError(
            "CALL gds.alpha.graph.graphProperty.drop($graph, 'porp')",
            Map.of("graph", DEFAULT_GRAPH_NAME),
            "The specified graph property 'porp' does not exist. Did you mean: ['prop']."
        );
    }
}

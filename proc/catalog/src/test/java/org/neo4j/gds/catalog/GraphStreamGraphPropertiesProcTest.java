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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.properties.graph.DoubleGraphPropertyValues;
import org.neo4j.gds.api.properties.graph.GraphPropertyValues;
import org.neo4j.gds.api.properties.graph.LongArrayGraphPropertyValues;
import org.neo4j.gds.api.properties.graph.LongGraphPropertyValues;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.extension.Neo4jGraph;

import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.neo4j.gds.compat.MapUtil.map;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

class GraphStreamGraphPropertiesProcTest extends BaseProcTest {

    @Neo4jGraph
    private static final String GRAPH = "CREATE (a)";

    private GraphStore graphStore;


    @BeforeEach
    void setup() throws Exception {
        registerProcedures(GraphProjectProc.class, GraphStreamGraphPropertiesProc.class);

        runQuery(GdsCypher.call(DEFAULT_GRAPH_NAME).graphProject().withAnyLabel().withAnyRelationshipType().yields());

        graphStore = GraphStoreCatalog.get("", DatabaseId.of(db), DEFAULT_GRAPH_NAME).graphStore();
    }

    @AfterEach
    void tearDown() {
        GraphStoreCatalog.removeAllLoadedGraphs();
    }

    @ParameterizedTest
    @MethodSource("org.neo4j.gds.catalog.GraphStreamGraphPropertiesProcTest#graphPropertyValues")
    void streamLoadedGraphProperty(GraphPropertyValues values) {
        graphStore.addGraphProperty("prop", values);

        String graphWriteQuery = formatWithLocale(
            "CALL gds.alpha.graph.graphProperty.stream('%s', 'prop')" + " YIELD propertyValue" + " RETURN propertyValue",
            DEFAULT_GRAPH_NAME
        );

        assertCypherResult(
            graphWriteQuery,
            values.objects().map(property -> map("propertyValue", property)).collect(Collectors.toList())
        );
    }

    static Stream<Arguments> graphPropertyValues() {
        return Stream.of(arguments(new LongGraphPropertyValues() {
            @Override
            public LongStream longValues() {
                return LongStream.range(0, 10);
            }

            @Override
            public long size() {
                return 10;

            }
        }), arguments(new DoubleGraphPropertyValues() {
            @Override
            public DoubleStream doubleValues() {
                return DoubleStream.of(42.0, 1.337, Double.NaN);
            }

            @Override
            public long size() {
                return 3;
            }
        }), arguments(new LongArrayGraphPropertyValues() {
            @Override
            public Stream<long[]> longArrayValues() {
                return Stream.of(new long[]{1L, 2L, 3L, 4L}, new long[]{42, 1337});
            }

            @Override
            public long size() {
                return 2;
            }
        }));
    }

    @Test
    void shouldFailOnNonExistingNodeProperty() {
        assertError(
            "CALL gds.alpha.graph.graphProperty.stream($graph, 'UNKNOWN')",
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
            "CALL gds.alpha.graph.graphProperty.stream($graph, 'porp')",
            Map.of("graph", DEFAULT_GRAPH_NAME),
            "The specified graph property 'porp' does not exist. Did you mean: ['prop']."
        );
    }
}

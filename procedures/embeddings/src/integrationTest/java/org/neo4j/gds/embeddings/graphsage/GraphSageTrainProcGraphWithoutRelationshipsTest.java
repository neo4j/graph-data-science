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
package org.neo4j.gds.embeddings.graphsage;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.PropertyMapping;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.extension.Neo4jGraph;
import org.neo4j.gds.extension.Neo4jModelCatalogExtension;
import org.neo4j.graphdb.QueryExecutionException;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Neo4jModelCatalogExtension
class GraphSageTrainProcGraphWithoutRelationshipsTest extends BaseProcTest {

    @Neo4jGraph
    private static final String DB_CYPHER =
        "CREATE" +
        "  (a:King{ name: 'A', age: 20 })" +
        ", (b:King{ name: 'B', age: 12 })";

    private static final String GRAPH_NAME = "embeddingsGraph";

    @BeforeEach
    void setup() throws Exception {
        registerProcedures(
            GraphProjectProc.class,
            GraphSageTrainProc.class
        );

        String query = GdsCypher.call(GRAPH_NAME)
            .graphProject()
            .withAnyLabel()
            .withNodeProperty(PropertyMapping.of("age", 1.0))
            .withAnyRelationshipType()
            .yields();

        runQuery(query);
    }

    @AfterEach
    void tearDown() {
        GraphStoreCatalog.removeAllLoadedGraphs();
    }

    @Test
    void failsWhenThereAreNoRelationshipsInTheGraph() {
        String modelName = "gsModel";
        String train = GdsCypher.call(GRAPH_NAME)
            .algo("gds.beta.graphSage")
            .trainMode()
            .addParameter("featureProperties", List.of("age"))
            .addParameter("embeddingDimension", 1)
            .addParameter("modelName", modelName)
            .yields();

        assertThatThrownBy(() -> runQuery(train))
            .isInstanceOf(QueryExecutionException.class)
            .hasRootCauseInstanceOf(IllegalArgumentException.class)
            .hasMessageEndingWith("There should be at least one relationship in the graph.");
    }
}

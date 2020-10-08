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
package org.neo4j.gds.embeddings.graphsage.proc;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.BaseProcTest;
import org.neo4j.graphalgo.GdsCypher;
import org.neo4j.graphalgo.Orientation;
import org.neo4j.graphalgo.PropertyMapping;
import org.neo4j.graphalgo.RelationshipProjection;
import org.neo4j.graphalgo.catalog.GraphCreateProc;
import org.neo4j.graphalgo.core.loading.GraphStoreCatalog;
import org.neo4j.graphalgo.core.model.ModelCatalog;
import org.neo4j.graphalgo.model.catalog.ModelDropProc;
import org.neo4j.graphalgo.model.catalog.ModelExistsProc;
import org.neo4j.graphdb.QueryExecutionException;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

class GraphSageTrainProcGraphWithoutRelationshipsTest extends BaseProcTest {

    private static final String DB_CYPHER =
            "CREATE" +
            "  (a:King{ name: 'A', age: 20 })" +
            ", (b:King{ name: 'B', age: 12 })" +
            ", (c:King{ name: 'C', age: 67 })" +
            ", (d:King{ name: 'D', age: 78 })" +
            ", (e:King{ name: 'E', age: 32 })" +
            ", (f:King{ name: 'F', age: 32 })" +
            ", (g:King{ name: 'G', age: 35 })" +
            ", (h:King{ name: 'H', age: 56 })" +
            ", (i:King{ name: 'I', age: 62 })" +
            ", (j:King{ name: 'J', age: 44 })" +
            ", (k:King{ name: 'K', age: 89 })" +
            ", (l:King{ name: 'L', age: 99 })" +
            ", (m:King{ name: 'M', age: 99 })" +
            ", (n:King{ name: 'N', age: 99 })" +
            ", (o:King{ name: 'O', age: 99 })";

    private static final String GRAPH_NAME = "embeddingsGraph";

    @BeforeEach
    void setup() throws Exception {
        registerProcedures(
            GraphCreateProc.class,
            GraphSageStreamProc.class,
            GraphSageWriteProc.class,
            GraphSageTrainProc.class,
            ModelExistsProc.class,
            ModelDropProc.class
        );

        runQuery(DB_CYPHER);

        String query = GdsCypher.call()
            .withNodeLabel("King")
            .withNodeProperty(PropertyMapping.of("age", 1.0))
            .withRelationshipType(
                "R",
                RelationshipProjection.of(
                    "*",
                    Orientation.UNDIRECTED
                )
            )
            .graphCreate(GRAPH_NAME)
            .yields();

        runQuery(query);
    }

    @AfterEach
    void tearDown() {
        GraphStoreCatalog.removeAllLoadedGraphs();
        ModelCatalog.removeAllLoadedModels();
    }

    @Test
    void failsWhenThereAreNoRelationshipsInTheGraph() {
        String modelName = "gsModel";
        String train = GdsCypher.call().explicitCreation(GRAPH_NAME)
            .algo("gds.alpha.graphSage")
            .trainMode()
            .addParameter("nodePropertyNames", List.of("age"))
            .addParameter("embeddingDimension", 1)
            .addParameter("modelName", modelName)
            .yields();

        assertThatThrownBy(() -> runQuery(train))
            .isInstanceOf(QueryExecutionException.class)
            .hasRootCauseInstanceOf(IllegalArgumentException.class)
            .hasMessageEndingWith("There should be at least one relationship in the graph");
    }
}

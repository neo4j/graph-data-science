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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSageTrainConfig;
import org.neo4j.graphalgo.BaseProcTest;
import org.neo4j.graphalgo.GdsCypher;
import org.neo4j.graphalgo.Orientation;
import org.neo4j.graphalgo.PropertyMapping;
import org.neo4j.graphalgo.api.DefaultValue;
import org.neo4j.graphalgo.catalog.GraphCreateProc;
import org.neo4j.graphalgo.core.model.ModelCatalog;

import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

class GraphSageEmptyRelationshipPropertyTest extends BaseProcTest {
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
        ", (o:King{ name: 'O', age: 99 })" +
        ", (a)-[:REL {weight: 0.001}]->(b)" +
        ", (a)-[:REL {weight: 0.001}]->(c)" +
        ", (b)-[:REL {weight: 0.001}]->(c)" +
        ", (b)-[:REL {weight: 0.001}]->(d)" +
        ", (c)-[:REL {weight: 0.001}]->(e)" +
        ", (d)-[:REL {weight: 0.001}]->(e)" +
        ", (d)-[:REL {weight: 0.001}]->(f)" +
        ", (e)-[:REL {weight: 0.001}]->(f)" +
        ", (e)-[:REL {weight: 0.001}]->(g)" +
        ", (h)-[:REL {weight: 0.001}]->(i)" +
        ", (i)-[:REL {weight: 0.001}]->(j)" +
        ", (j)-[:REL {weight: 0.001}]->(k)" +
        ", (j)-[:REL {weight: 0.001}]->(l)" +
        ", (k)-[:REL]->(l)";

    private static final String graphName = "weightedGsGraph";

    private static final  String modelName = "weightedGsModel";

    private static final  String relationshipWeightProperty = "weight";

    @BeforeEach
    void setup() throws Exception {
        registerProcedures(
            GraphCreateProc.class,
            GraphSageTrainProc.class,
            GraphSageStreamProc.class,
            GraphSageMutateProc.class,
            GraphSageWriteProc.class
        );

        runQuery(DB_CYPHER);
    }

    @AfterEach
    void tearDown() {
        ModelCatalog.removeAllLoadedModels();
    }

    @Test
    void failOnRelationshipWithoutProperty() {
        String graphQuery = GdsCypher.call()
            .withNodeLabel("King")
            .withNodeProperty(PropertyMapping.of("age", 1.0))
            .withRelationshipType("REL", Orientation.UNDIRECTED)
            .withRelationshipProperty(relationshipWeightProperty)
            .graphCreate(graphName)
            .yields();

        runQuery(graphQuery);

        String train = GdsCypher.call().explicitCreation(graphName)
            .algo("gds.beta.graphSage")
            .trainMode()
            .addParameter("featureProperties", List.of("age"))
            .addParameter("relationshipWeightProperty", relationshipWeightProperty)
            .addParameter("modelName", modelName)
            .yields();

        assertError(train, "Found a relationship without the specified property. Consider using `defaultValue` when loading the graph.");
    }

    @Test
    void doesNotFailOnRelationshipWithoutPropertyWithDefaultValue() {
        String graphQuery = GdsCypher.call()
            .withNodeLabel("King")
            .withNodeProperty(PropertyMapping.of("age", 1.0))
            .withRelationshipType("REL", Orientation.UNDIRECTED)
            .withRelationshipProperty(relationshipWeightProperty, DefaultValue.of(1.5, true))
            .graphCreate(graphName)
            .yields();

        runQuery(graphQuery);

        String train = GdsCypher.call().explicitCreation(graphName)
            .algo("gds.beta.graphSage")
            .trainMode()
            .addParameter("featureProperties", List.of("age"))
            .addParameter("relationshipWeightProperty", relationshipWeightProperty)
            .addParameter("modelName", modelName)
            .yields();

        runQuery(train);

        var model = ModelCatalog.get(
            getUsername(),
            modelName,
            ModelData.class,
            GraphSageTrainConfig.class
        );

        var layers = model.data().layers();
        for (Layer layer : layers) {
            layer.weights().forEach(w -> {
                var data = w.data();
                for (int i = 0; i < data.totalSize(); i++) {
                    assertThat(data.dataAt(i)).isNotNaN();
                }
            });
        }
    }

    @ParameterizedTest(name = "{1}")
    @MethodSource("procQueries")
    void failOnRelationshipWithoutPropertyInInductiveSetting(String embeddingQuery, String mode) {
        String graphQuery = GdsCypher.call()
            .withNodeLabel("King")
            .withNodeProperty(PropertyMapping.of("age", 1.0))
            .withRelationshipType("REL", Orientation.UNDIRECTED)
            .withRelationshipProperty(relationshipWeightProperty, DefaultValue.of(1.5, true))
            .graphCreate(graphName)
            .yields();

        runQuery(graphQuery);

        String trainQuery = GdsCypher.call().explicitCreation(graphName)
            .algo("gds.beta.graphSage")
            .trainMode()
            .addParameter("featureProperties", List.of("age"))
            .addParameter("relationshipWeightProperty", relationshipWeightProperty)
            .addParameter("modelName", modelName)
            .yields();

        runQuery(trainQuery);

        String graphCreateForStream = GdsCypher.call()
            .withNodeLabel("King")
            .withNodeProperty(PropertyMapping.of("age", 1.0))
            .withRelationshipType("REL", Orientation.UNDIRECTED)
            .withRelationshipProperty(relationshipWeightProperty/*, DefaultValue.of(1.5, true)*/)
            .graphCreate("inductiveGraph")
            .yields();

        runQuery(graphCreateForStream);

        String streamQuery = GdsCypher.call().explicitCreation("inductiveGraph")
            .algo("gds.beta.graphSage")
            .streamMode()
            .addParameter("modelName", modelName)
            .yields();

        assertError(embeddingQuery, "Found a relationship without the specified property. Consider using `defaultValue` when loading the graph.");
    }

    private static Stream<Arguments> procQueries() {
        return Stream.of(
            Arguments.arguments(
                GdsCypher.call().explicitCreation("inductiveGraph")
                    .algo("gds.beta.graphSage")
                    .streamMode()
                    .addParameter("modelName", modelName)
                    .yields(),
                "stream"
            ),
            Arguments.arguments(
                GdsCypher.call().explicitCreation("inductiveGraph")
                    .algo("gds.beta.graphSage")
                    .mutateMode()
                    .addParameter("modelName", modelName)
                    .addParameter("mutateProperty", "embedding")
                    .yields(),
                "mutate"
            ),
            Arguments.arguments(
                GdsCypher.call().explicitCreation("inductiveGraph")
                    .algo("gds.beta.graphSage")
                    .writeMode()
                    .addParameter("modelName", modelName)
                    .addParameter("writeProperty", "embedding")
                    .yields(),
                "write"
            )
        );
    }

}

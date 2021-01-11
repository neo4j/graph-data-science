/*
 * Copyright (c) 2017-2021 "Neo4j,"
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
package org.neo4j.gds.ml.nodemodels.logisticregression;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.embeddings.graphsage.ddl4j.functions.Weights;
import org.neo4j.gds.embeddings.graphsage.ddl4j.tensor.Matrix;
import org.neo4j.gds.embeddings.graphsage.subgraph.LocalIdMap;
import org.neo4j.gds.ml.nodemodels.multiclasslogisticregression.MultiClassNLRData;
import org.neo4j.graphalgo.BaseProcTest;
import org.neo4j.graphalgo.GdsCypher;
import org.neo4j.graphalgo.api.DefaultValue;
import org.neo4j.graphalgo.api.schema.GraphSchema;
import org.neo4j.graphalgo.catalog.GraphCreateProc;
import org.neo4j.graphalgo.core.model.Model;
import org.neo4j.graphalgo.core.model.ModelCatalog;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.isA;
import static org.hamcrest.number.OrderingComparison.greaterThan;

class MultiClassNLRMutateProcTest extends BaseProcTest {

    private final String DB_CYPHER = "CREATE " +
            "  (n1:N {a: -1.36753705, b:  1.46853155})" +
            ", (n2:N {a: -1.45431768, b: -1.67820474})" +
            ", (n3:N {a: -0.34216825, b: -1.31498086})" +
            ", (n4:N {a: -0.60765016, b:  1.0186564})" +
            ", (n5:N {a: -0.48403364, b: -0.49152604})";

    @BeforeEach
    void setup() throws Exception {
        registerProcedures(GraphCreateProc.class, MultiClassNLRMutateProc.class);

        var classIdMap = new LocalIdMap();
        classIdMap.toMapped(0);
        classIdMap.toMapped(1);
        var model = Model.of(
            "",
            "model",
            "",
            GraphSchema.empty(),
            MultiClassNLRData.builder()
                .weights(new Weights<>(new Matrix(new double[]{
                    1.12730619, -0.84532386, 0.93216654,
                    -1.12730619, 0.84532386, 0.0
                }, 2, 3)))
                .nodePropertyKeys(List.of("a", "b"))
                .classIdMap(classIdMap)
                .build(),
            ImmutableNodeLogisticRegressionTrainConfig.builder().modelName("model").targetProperty("foo").build()
        );
        ModelCatalog.set(model);

        runQuery(DB_CYPHER);

        String loadQuery = GdsCypher.call()
            .withAnyLabel()
            .withAnyRelationshipType()
            .withNodeProperties(List.of("a", "b"), DefaultValue.of(Double.NaN))
            .graphCreate("g")
            .yields();

        runQuery(loadQuery);
    }

    @AfterEach
    void tearDown() {
        ModelCatalog.drop("", "model");
    }

    @Test
    void mutate() {
        var query = GdsCypher
            .call()
            .explicitCreation("g")
            .algo("gds.alpha.ml.node.logisticRegression.predict")
            .mutateMode()
            .addParameter("mutateProperty", "class")
            .addParameter("modelName", "model")
            .yields();

        assertCypherResult(query, List.of(Map.of(
            "nodePropertiesWritten", 5L,
            "mutateMillis", greaterThan(-1L),
            "computeMillis", greaterThan(-1L),
            "postProcessingMillis", greaterThan(-1L),
            "createMillis", greaterThan(-1L),
            "configuration", isA(Map.class)
        )));
    }

    @Test
    void mutateWithProbabilities() {
        var query = GdsCypher
            .call()
            .explicitCreation("g")
            .algo("gds.alpha.ml.node.logisticRegression.predict")
            .mutateMode()
            .addParameter("mutateProperty", "class")
            .addParameter("predictedProbabilityProperty", "probabilities")
            .addParameter("modelName", "model")
            .yields();

        assertCypherResult(query, List.of(Map.of(
            "nodePropertiesWritten", 10L,
            "mutateMillis", greaterThan(-1L),
            "computeMillis", greaterThan(-1L),
            "postProcessingMillis", greaterThan(-1L),
            "createMillis", greaterThan(-1L),
            "configuration", isA(Map.class)
        )));
    }

}

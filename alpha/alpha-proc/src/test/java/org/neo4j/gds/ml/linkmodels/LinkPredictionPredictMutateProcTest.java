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
package org.neo4j.gds.ml.linkmodels;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.embeddings.graphsage.ddl4j.functions.Weights;
import org.neo4j.gds.embeddings.graphsage.ddl4j.tensor.Matrix;
import org.neo4j.gds.ml.linkmodels.logisticregression.LinkFeatureCombiner;
import org.neo4j.gds.ml.linkmodels.logisticregression.LinkLogisticRegressionData;
import org.neo4j.graphalgo.BaseProcTest;
import org.neo4j.graphalgo.GdsCypher;
import org.neo4j.graphalgo.Orientation;
import org.neo4j.graphalgo.RelationshipType;
import org.neo4j.graphalgo.catalog.GraphCreateProc;
import org.neo4j.graphalgo.catalog.GraphStreamRelationshipPropertiesProc;
import org.neo4j.graphalgo.core.loading.GraphStoreCatalog;
import org.neo4j.graphalgo.core.model.Model;
import org.neo4j.graphalgo.core.model.ModelCatalog;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.isA;
import static org.hamcrest.number.OrderingComparison.greaterThan;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.graphalgo.Orientation.UNDIRECTED;

class LinkPredictionPredictMutateProcTest extends BaseProcTest {

    private static final String GRAPH =
        "CREATE " +
        "(a:N {a: 0}), " +
        "(b:N {a: 0}), " +
        "(c:N {a: 0}), " +
        "(d:N {a: 100}), " +
        "(e:N {a: 100}), " +
        "(f:N {a: 100}), " +
        "(g:N {a: 200}), " +
        "(h:N {a: 200}), " +
        "(i:N {a: 200}), " +
        "(j:N {a: 300}), " +
        "(k:N {a: 300}), " +
        "(l:N {a: 300}), " +
        "(m:N {a: 400}), " +
        "(n:N {a: 400}), " +
        "(o:N {a: 400}), " +
        "(x)-[:T]->(x)";    // we need a relationship type to do an undirected projection

    @BeforeEach
    void setUp() throws Exception {
        registerProcedures(LinkPredictionPredictMutateProc.class, GraphStreamRelationshipPropertiesProc.class, GraphCreateProc.class);
        runQuery(GRAPH);

        runQuery(createQuery("g", UNDIRECTED));
    }

    private String createQuery(String graphName, Orientation orientation) {
        return GdsCypher
            .call()
            .withNodeLabel("N")
            .withNodeProperty("a")
            .withRelationshipType("T", UNDIRECTED)
            .graphCreate(graphName)
            .yields();
    }

    @AfterEach
    void tearDown() {
        ModelCatalog.removeAllLoadedModels();
    }

    @Test
    void producesAModel() {
        var graphStore = GraphStoreCatalog
            .get(getUsername(), db.databaseId(), "g")
            .graphStore();
        ModelCatalog.set(Model.of(
            getUsername(),
            "model",
            LinkPredictionTrain.MODEL_TYPE,
            graphStore.schema(),
            LinkLogisticRegressionData
                .builder()
                .weights(new Weights<>(new Matrix(new double[]{-0.0016811290857949518, 7.441367814815001E-4}, 1, 2)))
                .linkFeatureCombiner(LinkFeatureCombiner.L2)
                .nodePropertyKeys(List.of("a"))
                .numberOfFeatures(2)
                .build(),
            ImmutableLinkPredictionTrainConfig.builder()
                .modelName("model")
                .validationFolds(1)
                .trainRelationshipType(RelationshipType.ALL_RELATIONSHIPS)
                .testRelationshipType(RelationshipType.ALL_RELATIONSHIPS)
                .featureProperties(List.of("a"))
                .build()
        ));

        var query =
            "CALL gds.alpha.ml.linkPrediction.predict.mutate('g', { " +
            "  mutateRelationshipType: 'PREDICTED', " +
            "  modelName: 'model', " +
            "  threshold: 0.5, " +
            "  topN: 9" +
            "})";

        assertCypherResult(query, List.of(Map.of(
            "createMillis", greaterThan(0L),
            "computeMillis", greaterThan(0L),
            "mutateMillis", greaterThan(0L),
            "postProcessingMillis", 0L,
            // we are writing undirected rels so we get 2x topN
            "relationshipsWritten", 18L,
            "configuration", isA(Map.class)
        )));

        assertTrue(graphStore.hasRelationshipProperty(List.of(RelationshipType.of("PREDICTED")), "probability"));
    }
}

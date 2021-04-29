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
import org.neo4j.gds.ml.linkmodels.logisticregression.LinkFeatureCombiners;
import org.neo4j.gds.ml.linkmodels.logisticregression.LinkLogisticRegressionData;
import org.neo4j.graphalgo.BaseProcTest;
import org.neo4j.graphalgo.GdsCypher;
import org.neo4j.graphalgo.Orientation;
import org.neo4j.graphalgo.RelationshipProjection;
import org.neo4j.graphalgo.RelationshipType;
import org.neo4j.graphalgo.api.schema.GraphSchema;
import org.neo4j.graphalgo.catalog.GraphCreateProc;
import org.neo4j.graphalgo.core.loading.GraphStoreCatalog;
import org.neo4j.graphalgo.core.model.Model;
import org.neo4j.graphalgo.core.model.ModelCatalog;

import java.util.List;
import java.util.Map;

import static org.neo4j.graphalgo.Orientation.UNDIRECTED;

class LinkPredictionPredictStreamProcTest extends BaseProcTest {

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
        "(o:N {a: 400})";

    @BeforeEach
    void setUp() throws Exception {
        registerProcedures(LinkPredictionPredictStreamProc.class, GraphCreateProc.class);
        runQuery(GRAPH);

        runQuery(createQuery("g", UNDIRECTED));
    }

    private String createQuery(String graphName, Orientation orientation) {
        return GdsCypher
            .call()
            .withNodeLabel("N")
            .withNodeProperty("a")
            .withRelationshipType("IGNORED", RelationshipProjection.of("*", orientation))
            .graphCreate(graphName)
            .yields();
    }

    @AfterEach
    void tearDown() {
        ModelCatalog.removeAllLoadedModels();
    }

    @Test
    void canPredict() {
        var graphStore = GraphStoreCatalog
            .get(getUsername(), db.databaseId(), "g")
            .graphStore();
        addModel("model", graphStore.schema());

        var query =
            "CALL gds.alpha.ml.linkPrediction.predict.stream('g', { " +
            "  modelName: 'model', " +
            "  threshold: 0.5, " +
            "  topN: 10" +
            "}) YIELD node1, node2, probability" +
            " RETURN node1, node2, probability" +
            " ORDER BY probability DESC, node1, node2";

        assertCypherResult(query, List.of(
            Map.of("node1", 0L, "node2", 12L, "probability", 0.5646362918030292),
            Map.of("node1", 0L, "node2", 13L, "probability", 0.5646362918030292),
            Map.of("node1", 0L, "node2", 14L, "probability", 0.5646362918030292),
            Map.of("node1", 1L, "node2", 12L, "probability", 0.5646362918030292),
            Map.of("node1", 1L, "node2", 13L, "probability", 0.5646362918030292),
            Map.of("node1", 1L, "node2", 14L, "probability", 0.5646362918030292),
            Map.of("node1", 2L, "node2", 12L, "probability", 0.5646362918030292),
            Map.of("node1", 2L, "node2", 13L, "probability", 0.5646362918030292),
            Map.of("node1", 2L, "node2", 14L, "probability", 0.5646362918030292),
            Map.of("node1", 0L, "node2", 10L, "probability", 0.5473576181430894)
        ));
    }

    @Test
    void requiresUndirectedGraph() {
        runQuery(createQuery("g2", Orientation.NATURAL));

        addModel("model", GraphSchema.empty());

        var trainQuery =
            "CALL gds.alpha.ml.linkPrediction.predict.stream('g2', { " +
            "  modelName: 'model', " +
            "  threshold: 0.5, " +
            "  topN: 9" +
            "})";

        assertError(trainQuery, "Procedure requires relationship projections to be UNDIRECTED.");
    }

    private void addModel(String modelName, GraphSchema graphSchema) {
        List<String> featureProperties = List.of("a");
        ModelCatalog.set(Model.of(
            getUsername(),
            modelName,
            LinkPredictionTrain.MODEL_TYPE,
            graphSchema,
            LinkLogisticRegressionData
                .builder()
                .weights(
                    new Weights<>(new Matrix(
                        new double[]{0.000001, 0.1}, 1, 2)
                    )
                )
                .linkFeatureCombiner(LinkFeatureCombiners.L2)
                .nodeFeatureDimension(2)
                .build(),
            ImmutableLinkPredictionTrainConfig.builder()
                .modelName(modelName)
                .validationFolds(1)
                .trainRelationshipType(RelationshipType.ALL_RELATIONSHIPS)
                .testRelationshipType(RelationshipType.ALL_RELATIONSHIPS)
                .featureProperties(featureProperties)
                .negativeClassWeight(1d)
                .build()
        ));
    }
}

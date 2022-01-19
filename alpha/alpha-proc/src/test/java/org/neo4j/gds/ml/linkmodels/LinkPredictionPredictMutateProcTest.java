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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.RelationshipProjection;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.schema.GraphSchema;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.catalog.GraphStreamRelationshipPropertiesProc;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.core.model.Model;
import org.neo4j.gds.core.model.ModelCatalog;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.Neo4jModelCatalogExtension;
import org.neo4j.gds.ml.core.functions.Weights;
import org.neo4j.gds.ml.core.tensor.Matrix;
import org.neo4j.gds.ml.linkmodels.logisticregression.LinkFeatureCombiners;
import org.neo4j.gds.ml.linkmodels.logisticregression.LinkLogisticRegressionData;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.hamcrest.Matchers.isA;
import static org.hamcrest.number.OrderingComparison.greaterThan;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.gds.Orientation.UNDIRECTED;
import static org.neo4j.gds.TestSupport.assertGraphEquals;
import static org.neo4j.gds.TestSupport.fromGdl;

@Neo4jModelCatalogExtension
class LinkPredictionPredictMutateProcTest extends BaseProcTest {

    private static final String GRAPH =
        "CREATE " +
        "(foo:A {id: 42, a: -1}), " +
        "(bar:A {id: 1337, a: -2}), " +
        "(a:N {id: 0, a: 0}), " +
        "(b:N {id: 1, a: 0}), " +
        "(c:N {id: 2, a: 0}), " +
        "(d:N {id: 3, a: 100}), " +
        "(e:N {id: 4, a: 100}), " +
        "(f:N {id: 5, a: 100}), " +
        "(g:N {id: 6, a: 200}), " +
        "(h:N {id: 7, a: 200}), " +
        "(i:N {id: 8, a: 200}), " +
        "(j:N {id: 9, a: 300}), " +
        "(k:N {id: 10, a: 300}), " +
        "(l:N {id: 11, a: 300}), " +
        "(m:N {id: 12, a: 400}), " +
        "(n:N {id: 13, a: 400}), " +
        "(o:N {id: 14, a: 400})";

    @Inject
    private ModelCatalog modelCatalog;

    @BeforeEach
    void setUp() throws Exception {
        registerProcedures(LinkPredictionPredictMutateProc.class, GraphStreamRelationshipPropertiesProc.class, GraphProjectProc.class);
        runQuery(GRAPH);

        runQuery(projectQuery("g", UNDIRECTED));
    }

    private String projectQuery(String graphName, Orientation orientation) {
        return GdsCypher
            .call(graphName)
            .graphProject()
            .withNodeLabels("A", "N")
            .withNodeProperty("a")
            .withNodeProperty("id")
            .withRelationshipType("IGNORED", RelationshipProjection.of("*", orientation))
            .yields();
    }

    @Test
    void canPredict() {
        var graphStore = GraphStoreCatalog
            .get(getUsername(), db.databaseId(), "g")
            .graphStore();
        addModel("model", graphStore.schema());

        var query = GdsCypher
            .call("g")
            .algo("gds.alpha.ml.linkPrediction.predict")
            .mutateMode()
            .addParameter("nodeLabels", List.of("N"))
            .addParameter("mutateRelationshipType", "PREDICTED")
            .addParameter("modelName", "model")
            .addParameter("threshold", 0.0)
            .addParameter("topN", 9)
            .yields();

        assertCypherResult(query, List.of(Map.of(
            "preProcessingMillis", greaterThan(-1L),
            "computeMillis", greaterThan(-1L),
            "mutateMillis", greaterThan(-1L),
            "postProcessingMillis", 0L,
            // we are writing undirected rels so we get 2x topN
            "relationshipsWritten", 18L,
            "configuration", isA(Map.class)
        )));

        assertTrue(graphStore.hasRelationshipProperty(RelationshipType.of("PREDICTED"), "probability"));


        Graph actualGraph = GraphStoreCatalog.get(getUsername(), db.databaseId(), "g").graphStore().getGraph(
            NodeLabel.of("N"), RelationshipType.of("PREDICTED"), Optional.of("probability"));
        assertGraphEquals(
            fromGdl( "CREATE" +
                     "(n0:N { a: 0, id: 0 }) " +
                     "(n1:N { a: 0, id: 1 }) " +
                     "(n2:N { a: 0, id: 2 }) " +
                     "(n3:N { a: 100, id: 3 }) " +
                     "(n4:N { a: 100, id: 4 }) " +
                     "(n5:N { a: 100, id: 5 }) " +
                     "(n6:N { a: 200, id: 6 }) " +
                     "(n7:N { a: 200, id: 7 }) " +
                     "(n8:N { a: 200, id: 8 }) " +
                     "(n9:N { a: 300, id: 9 }) " +
                     "(n10:N { a: 300, id: 10 }) " +
                     "(n11:N { a: 300, id: 11 }) " +
                     "(n12:N { a: 400, id: 12 }) " +
                     "(n13:N { a: 400, id: 13 }) " +
                     "(n14:N { a: 400, id: 14 }) " +
                     "(n0)-[{ w: 0.500186 }]->(n1)" +
                     "(n0)-[{ w: 0.500186 }]->(n2)" +
                     "(n1)-[{ w: 0.500186 }]->(n0)" +
                     "(n1)-[{ w: 0.500186 }]->(n2)" +
                     "(n2)-[{ w: 0.500186 }]->(n0)" +
                     "(n2)-[{ w: 0.500186 }]->(n1)" +
                     "(n3)-[{ w: 0.500186 }]->(n4)" +
                     "(n3)-[{ w: 0.500186 }]->(n5)" +
                     "(n4)-[{ w: 0.500186 }]->(n3)" +
                     "(n4)-[{ w: 0.500186 }]->(n5)" +
                     "(n5)-[{ w: 0.500186 }]->(n3)" +
                     "(n5)-[{ w: 0.500186 }]->(n4)" +
                     "(n6)-[{ w: 0.500186 }]->(n7)" +
                     "(n6)-[{ w: 0.500186 }]->(n8)" +
                     "(n7)-[{ w: 0.500186 }]->(n6)" +
                     "(n7)-[{ w: 0.500186 }]->(n8)" +
                     "(n8)-[{ w: 0.500186 }]->(n6)" +
                     "(n8)-[{ w: 0.500186 }]->(n7)"
            ), actualGraph);
    }
    @Test
    void requiresUndirectedGraph() {
        runQuery(projectQuery("g2", Orientation.NATURAL));

        addModel("model", GraphSchema.empty());

        var query = GdsCypher
            .call("g2")
            .algo("gds.alpha.ml.linkPrediction.predict")
            .mutateMode()
            .addParameter("mutateRelationshipType", "PREDICTED")
            .addParameter("modelName", "model")
            .addParameter("threshold", 0.5)
            .addParameter("topN", 9)
            .yields();

        assertError(query, "Procedure requires relationship projections to be UNDIRECTED.");
    }

    private void addModel(String modelName, GraphSchema graphSchema) {
        List<String> featureProperties = List.of("a");
        modelCatalog.set(Model.of(
            getUsername(),
            modelName,
            LinkPredictionTrain.MODEL_TYPE,
            graphSchema,
            LinkLogisticRegressionData
                .builder()
                .weights(
                    new Weights<>(new Matrix(
                        new double[]{-0.0016811290857949518, 7.441367814815001E-4}, 1, 2)
                    )
                )
                .linkFeatureCombiner(LinkFeatureCombiners.L2)
                .nodeFeatureDimension(2)
                .build(),
            ImmutableLinkPredictionTrainConfig.builder()
                .modelName("model")
                .validationFolds(1)
                .trainRelationshipType(RelationshipType.ALL_RELATIONSHIPS)
                .testRelationshipType(RelationshipType.ALL_RELATIONSHIPS)
                .featureProperties(featureProperties)
                .negativeClassWeight(1d)
                .build(),
            LinkPredictionModelInfo.defaultInfo()
        ));
    }
}

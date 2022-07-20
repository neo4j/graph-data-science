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
package org.neo4j.gds.ml.linkmodels.pipeline.predict;

import org.junit.jupiter.api.BeforeEach;
import org.neo4j.gds.AlgoBaseProc;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.api.DefaultValue;
import org.neo4j.gds.api.schema.GraphSchema;
import org.neo4j.gds.catalog.GraphListProc;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.core.model.Model;
import org.neo4j.gds.core.model.ModelCatalog;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.Neo4jGraph;
import org.neo4j.gds.extension.Neo4jModelCatalogExtension;
import org.neo4j.gds.ml.core.functions.Weights;
import org.neo4j.gds.ml.core.tensor.Matrix;
import org.neo4j.gds.ml.metrics.ModelCandidateStats;
import org.neo4j.gds.ml.models.logisticregression.ImmutableLogisticRegressionData;
import org.neo4j.gds.ml.models.logisticregression.LogisticRegressionTrainConfig;
import org.neo4j.gds.ml.pipeline.linkPipeline.LinkPredictionModelInfo;
import org.neo4j.gds.ml.pipeline.linkPipeline.LinkPredictionPredictPipeline;
import org.neo4j.gds.ml.pipeline.linkPipeline.linkfunctions.L2FeatureStep;
import org.neo4j.gds.ml.pipeline.linkPipeline.train.LinkPredictionTrainConfigImpl;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.neo4j.gds.ml.pipeline.linkPipeline.LinkPredictionTrainingPipeline.MODEL_TYPE;

@Neo4jModelCatalogExtension
abstract class LinkPredictionPipelineProcTestBase extends BaseProcTest {

    abstract Class<? extends AlgoBaseProc<?, ?, ?, ?>> getProcedureClazz();

    @Neo4jGraph
    static String GDL = "CREATE " +
                        "  (n0:N {a: 1.0, b: 0.8, c: 1.0})" +
                        ", (n1:N {a: 2.0, b: 1.0, c: 1.0})" +
                        ", (n2:N {a: 3.0, b: 1.5, c: 1.0})" +
                        ", (n3:N {a: 0.0, b: 2.8, c: 1.0})" +
                        ", (n4:N {a: 1.0, b: 0.9, c: 1.0})" +
                        ", (m0:M {a: 1.0, b: 0.8, c: 1.0})" +
                        ", (m1:M {a: 2.0, b: 1.0, c: 1.0})" +
                        ", (m2:M {a: 3.0, b: 1.5, c: 1.0})" +
                        ", (m3:M {a: 0.0, b: 2.8, c: 1.0})" +
                        ", (m4:M {a: 1.0, b: 0.9, c: 1.0})" +
                        ", (n1)-[:T]->(n2)" +
                        ", (n3)-[:T]->(n4)" +
                        ", (n1)-[:T]->(n3)" +
                        ", (n2)-[:T]->(n4)" +
                        ", (m1)-[:T]->(m2)" +
                        ", (m3)-[:T]->(m4)" +
                        ", (m1)-[:T]->(m3)" +
                        ", (m2)-[:T]->(m4)";

    @Inject
    private ModelCatalog modelCatalog;

    @BeforeEach
    void setup() throws Exception {
        registerProcedures(GraphListProc.class, GraphProjectProc.class, getProcedureClazz());

        withModelInCatalog();

        runQuery(projectQuery("g", Orientation.UNDIRECTED));
    }

    private void withModelInCatalog() {
        var weights = new double[]{2.0, 1.0, -3.0};
        var pipeline = LinkPredictionPredictPipeline.from(Stream.of(), Stream.of(new L2FeatureStep(List.of("a", "b", "c"))));

        var modelData = ImmutableLogisticRegressionData.of(
            2,
            new Weights<>(new Matrix(
                weights,
                1,
                weights.length
            )),
            Weights.ofVector(0.0)
        );

        modelCatalog.set(Model.of(
            getUsername(),
            "model",
            MODEL_TYPE,
            GraphSchema.empty(),
            modelData,
            LinkPredictionTrainConfigImpl.builder()
                .username(getUsername())
                .modelName("model")
                .pipeline("DUMMY")
                .sourceNodeLabel("N")
                .targetNodeLabel("N")
                .targetRelationshipType("T")
                .graphName("g")
                .negativeClassWeight(1.0)
                .build(),
            LinkPredictionModelInfo.of(
                Map.of(),
                Map.of(),
                ModelCandidateStats.of(LogisticRegressionTrainConfig.DEFAULT, Map.of(), Map.of()),
                pipeline
            )
        ));
    }

    String projectQuery(String graphName, Orientation orientation) {
        return GdsCypher.call(graphName)
            .graphProject()
            .withNodeLabels("N", "M")
            .withRelationshipType("T", orientation)
            .withNodeProperties(List.of("a", "b", "c"), DefaultValue.DEFAULT)
            .yields();
    }

}

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
package org.neo4j.gds.ml.linkmodels.pipeline;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.catalog.GraphCreateProc;
import org.neo4j.gds.catalog.GraphStreamNodePropertiesProc;
import org.neo4j.gds.ml.linkmodels.pipeline.linkFeatures.LinkFeatureStepFactory;
import org.neo4j.graphalgo.AlgoBaseProc;
import org.neo4j.graphalgo.BaseProcTest;
import org.neo4j.graphalgo.GdsCypher;
import org.neo4j.graphalgo.NodeLabel;
import org.neo4j.graphalgo.Orientation;
import org.neo4j.graphalgo.RelationshipType;
import org.neo4j.graphalgo.api.DefaultValue;
import org.neo4j.graphalgo.core.utils.paged.HugeObjectArray;
import org.neo4j.graphalgo.extension.Neo4jGraph;

import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.withPrecision;

class FeaturePipelineTest extends BaseProcTest {

    @Neo4jGraph
    private static final String GRAPH = "CREATE " +
        "(a:N {noise: 42, z: 13, array: [3.0,2.0]}), " +
        "(b:N {noise: 1337, z: 0, array: [1.0,1.0]}), " +
        "(c:N {noise: 42, z: 2, array: [8.0,2.3]}), " +
        "(d:N {noise: 42, z: 9, array: [0.1,91.0]}), " +
        "(e:Ignore {noise: 42, z: 9, array: [0.1,91.0]}), " +

        "(a)-[:REL]->(b), " +
        "(b)-[:IGNORE]->(c), " +
        "(a)-[:REL]->(c), " +
        "(a)-[:REL]->(d)";

    @BeforeEach
    void setup() throws Exception {
        registerProcedures(
            GraphCreateProc.class,
            GraphStreamNodePropertiesProc.class
        );
        String createQuery = GdsCypher.call()
            .withNodeLabel("N")
            .withNodeLabel("Ignore")
            .withRelationshipType("REL", Orientation.UNDIRECTED)
            .withRelationshipType("IGNORE", Orientation.UNDIRECTED)
            .withNodeProperties(List.of("noise", "z", "array"), DefaultValue.DEFAULT)
            .graphCreate("g")
            .yields();

        runQuery(createQuery);
    }

    // add several linkFeatureSteps + assert that linkFeatures computed correct
    @Test
    void singleLinkFeatureStep() {
        ProcedureTestUtils.applyOnProcedure(db, (Consumer<? super AlgoBaseProc<?, ?, ?>>) caller -> {
            var pipeline = new FeaturePipeline(caller, db.databaseId(), getUsername());

            pipeline.addLinkFeature(
                LinkFeatureStepFactory.HADAMARD.name(),
                Map.of("featureProperties", List.of("array"))
            );

            var expected = HugeObjectArray.of(
                new double[]{3 * 1D, 2 * 1D}, // a-b
                new double[]{3 * 8, 2 * 2.3D}, // a-c
                new double[]{3 * 0.1D, 2 * 91.0D}, // a-d
                new double[]{3 * 1D, 2 * 1D}, // a-b
                new double[]{3 * 8, 2 * 2.3D}, // a-c
                new double[]{3 * 0.1D, 2 * 91.0D} // a-d
            );
            var actual = computePropertiesAndLinkFeatures(pipeline, "g");

            assertThat(actual.size()).isEqualTo(expected.size());

            for (long i = 0; i < actual.size(); i++) {
                assertThat(actual.get(i)).containsExactly(expected.get(i), withPrecision(1e-4D));
            }
        });
    }

    @Test
    void multipleLinkFeatureStep() {
        ProcedureTestUtils.applyOnProcedure(db, (Consumer<? super AlgoBaseProc<?, ?, ?>>) caller -> {
            var pipeline = new FeaturePipeline(caller, db.databaseId(), getUsername());

            pipeline.addLinkFeature(
                LinkFeatureStepFactory.HADAMARD.name(),
                Map.of("featureProperties", List.of("array"))
            );
            pipeline.addLinkFeature(
                LinkFeatureStepFactory.COSINE.name(),
                Map.of("featureProperties", List.of("noise", "z"))
            );

            var normA = Math.sqrt(42 * 42 + 13 * 13);
            var normB = Math.sqrt(1337 * 1337 + 0 * 0);
            var normC = Math.sqrt(42 * 42 + 2 * 2);
            var normD = Math.sqrt(42 * 42 + 9 * 9);

            var expected = HugeObjectArray.of(
                new double[]{3 * 1D, 2 * 1D, (42 * 1337 + 13 * 0D) / normA / normB}, // a-b
                new double[]{3 * 8, 2 * 2.3D, (42 * 42 + 13 * 2) / normA / normC}, // a-c
                new double[]{3 * 0.1D, 2 * 91.0D, (42 * 42 + 13 * 9) / normA / normD}, // a-d
                new double[]{3 * 1D, 2 * 1D, (42 * 1337 + 13 * 0D) / normA / normB}, // a-b
                new double[]{3 * 8, 2 * 2.3D, (42 * 42 + 13 * 2) / normA / normC}, // a-c
                new double[]{3 * 0.1D, 2 * 91.0D, (42 * 42 + 13 * 9) / normA / normD} // a-d
            );

            var actual = computePropertiesAndLinkFeatures(pipeline, "g");

            assertThat(actual.size()).isEqualTo(expected.size());

            for (long i = 0; i < actual.size(); i++) {
                assertThat(actual.get(i)).containsExactly(expected.get(i), withPrecision(1e-4D));
            }
        });
    }

    @Test
    void testProcedureAndLinkFeatures() {
        ProcedureTestUtils.applyOnProcedure(db, (Consumer<? super AlgoBaseProc<?, ?, ?>>) caller -> {

            var pipeline = new FeaturePipeline(caller, db.databaseId(), getUsername());

            pipeline.addProcedureStep("pageRank", Map.of("mutateProperty", "pageRank"));
            pipeline.addLinkFeature(
                LinkFeatureStepFactory.HADAMARD.name(),
                Map.of("featureProperties", List.of("pageRank"))
            );

            var expectedPageRanks = List.of(
                1.8445425214324187,
                0.6668064514098416,
                0.6668064514098416,
                0.6668064514098416
            );

            var expected = HugeObjectArray.of(
                new double[]{expectedPageRanks.get(0) * expectedPageRanks.get(1)},
                new double[]{expectedPageRanks.get(0) * expectedPageRanks.get(2)},
                new double[]{expectedPageRanks.get(0) * expectedPageRanks.get(3)},
                new double[]{expectedPageRanks.get(1) * expectedPageRanks.get(0)},
                new double[]{expectedPageRanks.get(2) * expectedPageRanks.get(0)},
                new double[]{expectedPageRanks.get(3) * expectedPageRanks.get(0)}
            );

            var actual = computePropertiesAndLinkFeatures(pipeline, "g");

            assertThat(actual.size()).isEqualTo(expected.size());

            for (long i = 0; i < actual.size(); i++) {
                assertThat(actual.get(i)).containsExactly(expected.get(i), withPrecision(1e-4D));
            }
        });
    }

    @Test
    void validateLinkFeatureSteps() {
        ProcedureTestUtils.applyOnProcedure(db, (Consumer<? super AlgoBaseProc<?, ?, ?>>) caller -> {
            var pipeline = new FeaturePipeline(caller, db.databaseId(), getUsername());

            pipeline.addLinkFeature(
                LinkFeatureStepFactory.HADAMARD.name(),
                Map.of("featureProperties", List.of("noise", "no-property", "no-prop-2"))
            );
            pipeline.addLinkFeature(
                LinkFeatureStepFactory.HADAMARD.name(),
                Map.of("featureProperties", List.of("other-no-property"))
            );

            assertThatThrownBy(() -> computePropertiesAndLinkFeatures(pipeline, "g"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(
                    "Node properties [no-property, no-prop-2, other-no-property] defined in the LinkFeatureSteps do not exist in the graph or part of the pipeline");
        });
    }

    private HugeObjectArray<double[]> computePropertiesAndLinkFeatures(FeaturePipeline pipeline, String g) {
        pipeline.executeProcedureSteps(g, List.of(NodeLabel.of("N")), RelationshipType.of("REL"));
        return pipeline.computeFeatures(g, List.of(NodeLabel.of("N")), RelationshipType.of("REL"));
    }

}

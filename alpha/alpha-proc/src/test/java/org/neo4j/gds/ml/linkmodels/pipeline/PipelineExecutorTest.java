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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.ProcedureRunner;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.DefaultValue;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.catalog.GraphCreateProc;
import org.neo4j.gds.catalog.GraphStreamNodePropertiesProc;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.core.utils.paged.HugeObjectArray;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.extension.Neo4jGraph;
import org.neo4j.gds.louvain.LouvainMutateProc;
import org.neo4j.gds.ml.linkmodels.pipeline.linkFeatures.linkfunctions.CosineFeatureStep;
import org.neo4j.gds.ml.linkmodels.pipeline.linkFeatures.linkfunctions.HadamardFeatureStep;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.withPrecision;

class PipelineExecutorTest extends BaseProcTest {

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

    public static final String GRAPH_NAME = "g";
    public static final NodeLabel NODE_LABEL = NodeLabel.of("N");
    public static final RelationshipType RELATIONSHIP_TYPE = RelationshipType.of("REL");

    private GraphStore graphStore;

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
            .graphCreate(GRAPH_NAME)
            .yields();

        runQuery(createQuery);

        graphStore = GraphStoreCatalog.get(getUsername(), db.databaseId(), GRAPH_NAME).graphStore();
    }

    // add several linkFeatureSteps + assert that linkFeatures computed correct
    @Test
    void singleLinkFeatureStep() {
        var pipeline = new TrainingPipeline();
        pipeline.addFeatureStep(new HadamardFeatureStep(List.of("array")));

        ProcedureRunner.applyOnProcedure(db, LouvainMutateProc.class, caller -> {
            var actual = computePropertiesAndLinkFeatures(
                new PipelineExecutor(pipeline, caller, db.databaseId(), getUsername(), GRAPH_NAME, ProgressTracker.NULL_TRACKER)
            );

            var expected = HugeObjectArray.of(
                new double[]{3 * 1D, 2 * 1D}, // a-b
                new double[]{3 * 8, 2 * 2.3D}, // a-c
                new double[]{3 * 0.1D, 2 * 91.0D}, // a-d
                new double[]{3 * 1D, 2 * 1D}, // a-b
                new double[]{3 * 8, 2 * 2.3D}, // a-c
                new double[]{3 * 0.1D, 2 * 91.0D} // a-d
            );

            assertThat(actual.size()).isEqualTo(expected.size());

            for (long i = 0; i < actual.size(); i++) {
                assertThat(actual.get(i)).containsExactly(expected.get(i), withPrecision(1e-4D));
            }
        });
    }

    @Test
    void dependentNodePropertySteps() {
        var pipeline = new TrainingPipeline();

        pipeline.addNodePropertyStep(NodePropertyStep.of("degree", Map.of("mutateProperty", "degree")));
        pipeline.addNodePropertyStep(NodePropertyStep.of("scaleProperties", Map.of(
            "mutateProperty", "nodeFeatures",
            "nodeProperties", "degree",
            "scaler", "MEAN"
        )));

        ProcedureRunner.applyOnProcedure(db, LouvainMutateProc.class, caller -> {
            new PipelineExecutor(pipeline, caller, db.databaseId(), getUsername(), GRAPH_NAME, ProgressTracker.NULL_TRACKER).executeNodePropertySteps(
                NodeLabel.listOf("N"),
                RELATIONSHIP_TYPE
            );

            assertThat(graphStore.nodePropertyKeys(NODE_LABEL)).contains("degree", "nodeFeatures");
        });
    }

    @Test
    void multipleLinkFeatureStep() {
        var pipeline = new TrainingPipeline();
        pipeline.addFeatureStep(new HadamardFeatureStep(List.of("array")));
        pipeline.addFeatureStep(new CosineFeatureStep(List.of("noise", "z")));

        var normA = Math.sqrt(42 * 42 + 13 * 13);
        var normB = Math.sqrt(1337 * 1337 + 0 * 0);
        var normC = Math.sqrt(42 * 42 + 2 * 2);
        var normD = Math.sqrt(42 * 42 + 9 * 9);


        ProcedureRunner.applyOnProcedure(db, LouvainMutateProc.class, caller -> {
            var actual = computePropertiesAndLinkFeatures(new PipelineExecutor(
                pipeline,
                caller,
                db.databaseId(),
                getUsername(),
                GRAPH_NAME,
                ProgressTracker.NULL_TRACKER
            ));

            var expected = HugeObjectArray.of(
                new double[]{3 * 1D, 2 * 1D, (42 * 1337 + 13 * 0D) / normA / normB}, // a-b
                new double[]{3 * 8, 2 * 2.3D, (42 * 42 + 13 * 2) / normA / normC}, // a-c
                new double[]{3 * 0.1D, 2 * 91.0D, (42 * 42 + 13 * 9) / normA / normD}, // a-d
                new double[]{3 * 1D, 2 * 1D, (42 * 1337 + 13 * 0D) / normA / normB}, // a-b
                new double[]{3 * 8, 2 * 2.3D, (42 * 42 + 13 * 2) / normA / normC}, // a-c
                new double[]{3 * 0.1D, 2 * 91.0D, (42 * 42 + 13 * 9) / normA / normD} // a-d
            );

            assertThat(actual.size()).isEqualTo(expected.size());

            for (long i = 0; i < actual.size(); i++) {
                assertThat(actual.get(i)).containsExactly(expected.get(i), withPrecision(1e-4D));
            }
        });
    }

    @Test
    void testProcedureAndLinkFeatures() {
        var pipeline = new TrainingPipeline();
        pipeline.addNodePropertyStep(NodePropertyStep.of(
            "pageRank",
            Map.of("mutateProperty", "pageRank")
        ));
        pipeline.addFeatureStep(new HadamardFeatureStep(List.of("pageRank")));

        var expectedPageRanks = List.of(
            1.8445425214324187,
            0.6668064514098416,
            0.6668064514098416,
            0.6668064514098416
        );

        ProcedureRunner.applyOnProcedure(db, LouvainMutateProc.class, caller -> {
            var actual = computePropertiesAndLinkFeatures(new PipelineExecutor(
                pipeline,
                caller,
                db.databaseId(),
                getUsername(),
                GRAPH_NAME,
                ProgressTracker.NULL_TRACKER
            ));

            var expected = HugeObjectArray.of(
                new double[]{expectedPageRanks.get(0) * expectedPageRanks.get(1)},
                new double[]{expectedPageRanks.get(0) * expectedPageRanks.get(2)},
                new double[]{expectedPageRanks.get(0) * expectedPageRanks.get(3)},
                new double[]{expectedPageRanks.get(1) * expectedPageRanks.get(0)},
                new double[]{expectedPageRanks.get(2) * expectedPageRanks.get(0)},
                new double[]{expectedPageRanks.get(3) * expectedPageRanks.get(0)}
            );

            assertThat(actual.size()).isEqualTo(expected.size());

            for (long i = 0; i < actual.size(); i++) {
                assertThat(actual.get(i)).containsExactly(expected.get(i), withPrecision(1e-4D));
            }
        });
    }

    @Test
    void validateLinkFeatureSteps() {
        var pipeline = new TrainingPipeline();
        pipeline.addFeatureStep(new HadamardFeatureStep(List.of("noise", "no-property", "no-prop-2")));
        pipeline.addFeatureStep(new HadamardFeatureStep(List.of("other-no-property")));

        ProcedureRunner.applyOnProcedure(db, LouvainMutateProc.class, caller -> {
            var executor = new PipelineExecutor(
                pipeline,
                caller,
                db.databaseId(),
                getUsername(),
                GRAPH_NAME,
                ProgressTracker.NULL_TRACKER
            );

            assertThatThrownBy(() -> computePropertiesAndLinkFeatures(executor))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(
                    "Node properties [no-property, no-prop-2, other-no-property] defined in the LinkFeatureSteps do not exist in the graph or part of the pipeline");
        });
    }

    public static Stream<Arguments> invalidSplits() {
        return Stream.of(
            Arguments.of(
                LinkPredictionSplitConfig.builder().testFraction(0.01).build(),
                "Test graph contains no relationships. Consider increasing the `testFraction` or provide a larger graph"
            ),
            Arguments.of(
                LinkPredictionSplitConfig.builder().trainFraction(0.01).testFraction(0.5).build(),
                "Train graph contains no relationships. Consider increasing the `trainFraction` or provide a larger graph"
            )
            // If the trainFraction is scaled to the total relationship count, this will fail
//            Arguments.of(
//                LinkPredictionSplitConfig.builder().trainFraction(0.5).testFraction(0.5).build(),
//                "Feature graph contains no relationships. Consider decreasing `testFraction` or `trainFraction` or provide a larger graph."
//            )
        );
    }

    @ParameterizedTest
    @MethodSource("invalidSplits")
    void failOnEmptySplitGraph(LinkPredictionSplitConfig splitConfig, String expectedError) {
        var pipeline = new TrainingPipeline();
        pipeline.setSplitConfig(splitConfig);

        ProcedureRunner.applyOnProcedure(db, LouvainMutateProc.class, caller -> {
            var executor = new PipelineExecutor(
                pipeline,
                caller,
                db.databaseId(),
                getUsername(),
                GRAPH_NAME,
                ProgressTracker.NULL_TRACKER
            );

            assertThatThrownBy(() -> executor.splitRelationships(
                graphStore,
                List.of("*"),
                List.of(NODE_LABEL.name),
                Optional.of(42L)
            ))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage(expectedError);
        });
    }

    @Test
    void failOnExistingSplitRelTypes() {
        var graphName = "invalidGraph";

        String createQuery = GdsCypher.call()
            .withAnyLabel()
            .withRelationshipType("_TEST_", "REL")
            .withRelationshipType("_TEST_COMPLEMENT_", "IGNORE")
            .graphCreate(graphName)
            .yields();

        runQuery(createQuery);

        var invalidGraphStore = GraphStoreCatalog.get(getUsername(), db.databaseId(), graphName).graphStore();

        var pipeline = new TrainingPipeline();
        pipeline.setSplitConfig(LinkPredictionSplitConfig.builder().build());

        ProcedureRunner.applyOnProcedure(db, LouvainMutateProc.class, caller -> {
            var executor = new PipelineExecutor(
                pipeline,
                caller,
                db.databaseId(),
                getUsername(),
                graphName,
                ProgressTracker.NULL_TRACKER
            );

            assertThatThrownBy(() -> executor.splitRelationships(
                invalidGraphStore,
                List.of("*"),
                List.of(NODE_LABEL.name),
                Optional.of(42L)
            ))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("The relationship types ['_TEST_', '_TEST_COMPLEMENT_'] are in the input graph, but are reserved for splitting.");
        });
    }

    private HugeObjectArray<double[]> computePropertiesAndLinkFeatures(PipelineExecutor pipeline) {
        pipeline.executeNodePropertySteps(List.of(NODE_LABEL), RELATIONSHIP_TYPE);
        return pipeline.computeFeatures(List.of(NODE_LABEL), RELATIONSHIP_TYPE, 4);
    }

}

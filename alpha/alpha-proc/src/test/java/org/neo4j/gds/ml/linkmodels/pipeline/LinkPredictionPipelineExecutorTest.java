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
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.TestProcedureRunner;
import org.neo4j.gds.api.DefaultValue;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.catalog.GraphCreateProc;
import org.neo4j.gds.catalog.GraphStreamNodePropertiesProc;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.extension.Neo4jGraph;
import org.neo4j.gds.louvain.LouvainMutateProc;
import org.neo4j.gds.ml.linkmodels.pipeline.linkFeatures.linkfunctions.HadamardFeatureStep;
import org.neo4j.gds.ml.linkmodels.pipeline.train.ImmutableLinkPredictionTrainConfig;
import org.neo4j.gds.test.TestProc;

import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

class LinkPredictionPipelineExecutorTest extends BaseProcTest {

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

// TODO: this is an integration test
//
//    @Test
//    void testProcedureAndLinkFeatures() {
//        var pipeline = new LinkPredictionPipeline();
//        pipeline.addNodePropertyStep(NodePropertyStep.of(
//            "pageRank",
//            Map.of("mutateProperty", "pageRank")
//        ));
//        pipeline.addFeatureStep(new HadamardFeatureStep(List.of("pageRank")));
//
//        var expectedPageRanks = List.of(
//            1.8445425214324187,
//            0.6668064514098416,
//            0.6668064514098416,
//            0.6668064514098416
//        );
//
//        TestProcedureRunner.applyOnProcedure(db, LouvainMutateProc.class, caller -> {
//            var actual = computePropertiesAndLinkFeatures(new LinkPredictionPipelineExecutor(
//                pipeline,
//                caller,
//                db.databaseId(),
//                getUsername(),
//                GRAPH_NAME,
//                ProgressTracker.NULL_TRACKER
//            ));
//
//            var expected = HugeObjectArray.of(
//                new double[]{expectedPageRanks.get(0) * expectedPageRanks.get(1)},
//                new double[]{expectedPageRanks.get(0) * expectedPageRanks.get(2)},
//                new double[]{expectedPageRanks.get(0) * expectedPageRanks.get(3)},
//                new double[]{expectedPageRanks.get(1) * expectedPageRanks.get(0)},
//                new double[]{expectedPageRanks.get(2) * expectedPageRanks.get(0)},
//                new double[]{expectedPageRanks.get(3) * expectedPageRanks.get(0)}
//            );
//
//            assertThat(actual.size()).isEqualTo(expected.size());
//
//            for (long i = 0; i < actual.size(); i++) {
//                assertThat(actual.get(i)).containsExactly(expected.get(i), withPrecision(1e-4D));
//            }
//        });
//    }

    @Test
    void validateLinkFeatureSteps() {
        var pipeline = new LinkPredictionPipeline();
        pipeline.addFeatureStep(new HadamardFeatureStep(List.of("noise", "no-property", "no-prop-2")));
        pipeline.addFeatureStep(new HadamardFeatureStep(List.of("other-no-property")));

        TestProcedureRunner.applyOnProcedure(db, TestProc.class, caller -> {
            var executor = new LinkPredictionPipelineExecutor(
                pipeline,
                ImmutableLinkPredictionTrainConfig.builder().modelName("foo").pipeline("bar").build(),
                caller,
                graphStore,
                GRAPH_NAME,
                ProgressTracker.NULL_TRACKER
            );

            assertThatThrownBy(executor::compute)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(
                    "Node properties [no-property, no-prop-2, other-no-property] defined in the LinkFeatureSteps do not exist in the graph or part of the pipeline");
        });
    }

    static Stream<Arguments> invalidSplits() {
        return Stream.of(
            Arguments.of(
                LinkPredictionSplitConfig.builder().testFraction(0.01).build(),
                "Test graph contains no relationships. Consider increasing the `testFraction` or provide a larger graph"
            ),
            Arguments.of(
                LinkPredictionSplitConfig.builder().trainFraction(0.01).testFraction(0.5).build(),
                "Train graph contains no relationships. Consider increasing the `trainFraction` or provide a larger graph"
            )
        );
    }

    @ParameterizedTest
    @MethodSource("invalidSplits")
    void failOnEmptySplitGraph(LinkPredictionSplitConfig splitConfig, String expectedError) {
        var pipeline = new LinkPredictionPipeline();
        pipeline.setSplitConfig(splitConfig);

        var linkPredictionTrainConfig = ImmutableLinkPredictionTrainConfig.builder()
            .modelName("foo")
            .pipeline("bar")
            .addNodeLabel(NODE_LABEL.name)
            .build();

        TestProcedureRunner.applyOnProcedure(db, LouvainMutateProc.class, caller -> {
            var executor = new LinkPredictionPipelineExecutor(
                pipeline,
                linkPredictionTrainConfig,
                caller,
                graphStore,
                GRAPH_NAME,
                ProgressTracker.NULL_TRACKER
            );

            assertThatThrownBy(executor::compute)
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

        var pipeline = new LinkPredictionPipeline();
        pipeline.setSplitConfig(LinkPredictionSplitConfig.builder().build());

        var linkPredictionTrainConfig = ImmutableLinkPredictionTrainConfig.builder()
            .modelName("foo")
            .pipeline("bar")
            .addNodeLabel(NODE_LABEL.name)
            .build();

        TestProcedureRunner.applyOnProcedure(db, LouvainMutateProc.class, caller -> {
            var executor = new LinkPredictionPipelineExecutor(
                pipeline,
                linkPredictionTrainConfig,
                caller,
                invalidGraphStore,
                graphName,
                ProgressTracker.NULL_TRACKER
            );

            assertThatThrownBy(executor::compute)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("The relationship types ['_TEST_', '_TEST_COMPLEMENT_'] are in the input graph, but are reserved for splitting.");
        });
    }

}

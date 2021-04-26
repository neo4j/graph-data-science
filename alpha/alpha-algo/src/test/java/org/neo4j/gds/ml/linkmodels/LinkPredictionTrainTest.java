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

import org.assertj.core.api.AssertionsForInterfaceTypes;
import org.assertj.core.data.Percentage;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.ml.linkmodels.metrics.LinkMetric;
import org.neo4j.graphalgo.Orientation;
import org.neo4j.graphalgo.RelationshipType;
import org.neo4j.graphalgo.TestProgressLogger;
import org.neo4j.graphalgo.api.CSRGraph;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.GraphStore;
import org.neo4j.graphalgo.core.huge.UnionGraph;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.core.utils.progress.EmptyProgressEventTracker;
import org.neo4j.graphalgo.extension.GdlExtension;
import org.neo4j.graphalgo.extension.GdlGraph;
import org.neo4j.graphalgo.extension.Inject;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.graphalgo.TestLog.INFO;
import static org.neo4j.graphalgo.assertj.Extractors.removingThreadId;

@GdlExtension
class LinkPredictionTrainTest {

    // Five cliques of size 2, 3, or 4
    @GdlGraph(orientation = Orientation.UNDIRECTED)
    static String GRAPH =
        "(a:N {z: 0, array: [1.0,2.0,3.0,4.0,5.0]}), " +
        "(b:N {z: 0, array: [1.0,2.0,3.0,4.0,5.0]}), " +
        "(c:N {z: 0, array: [1.0,2.0,3.0,4.0,5.0]}), " +
        "(d:N {z: 0, array: [1.0,2.0,3.0,4.0,5.0]}), " +
        "(e:N {z: 100, array: [-1.0,2.0,3.0,4.0,5.0]}), " +
        "(f:N {z: 100, array: [-1.0,2.0,3.0,4.0,5.0]}), " +
        "(g:N {z: 100, array: [-1.0,2.0,3.0,4.0,5.0]}), " +
        "(h:N {z: 200, array: [-1.0,-2.0,3.0,4.0,5.0]}), " +
        "(i:N {z: 200, array: [-1.0,-2.0,3.0,4.0,5.0]}), " +
        "(j:N {z: 300, array: [-1.0,2.0,3.0,-4.0,5.0]}), " +
        "(k:N {z: 300, array: [-1.0,2.0,3.0,-4.0,5.0]}), " +
        "(l:N {z: 300, array: [-1.0,2.0,3.0,-4.0,5.0]}), " +
        "(m:N {z: 400, array: [1.0,2.0,-3.0,4.0,-5.0]}), " +
        "(n:N {z: 400, array: [1.0,2.0,-3.0,4.0,-5.0]}), " +
        "(o:N {z: 400, array: [1.0,2.0,-3.0,4.0,-5.0]}), " +

        "(a)-[:TRAIN {label: 1}]->(b), " +
        "(a)-[:TEST {label: 1}]->(c), " +       // selected for test
        "(a)-[:TRAIN {label: 1}]->(d), " +
        "(b)-[:TRAIN {label: 1}]->(c), " +
        "(b)-[:TEST {label: 1}]->(d), " +       // selected for test
        "(c)-[:TRAIN {label: 1}]->(d), " +

        "(e)-[:TEST {label: 1}]->(f), " +       // selected for test
        "(e)-[:TRAIN {label: 1}]->(g), " +
        "(f)-[:TRAIN {label: 1}]->(g), " +

        "(h)-[:TRAIN {label: 1}]->(i), " +

        "(j)-[:TRAIN {label: 1}]->(k), " +
        "(j)-[:TRAIN {label: 1}]->(l), " +
        "(k)-[:TRAIN {label: 1}]->(l), " +

        "(m)-[:TEST {label: 1}]->(n), " +       // selected for test
        "(m)-[:TEST {label: 1}]->(o), " +       // selected for test
        "(n)-[:TRAIN {label: 1}]->(o), " +
        // 11 false positive TRAIN rels
        "(a)-[:TRAIN {label: 0}]->(e), " +
        "(a)-[:TRAIN {label: 0}]->(o), " +
        "(b)-[:TRAIN {label: 0}]->(e), " +
        "(e)-[:TRAIN {label: 0}]->(i), " +
        "(e)-[:TRAIN {label: 0}]->(o), " +
        "(e)-[:TRAIN {label: 0}]->(n), " +
        "(h)-[:TRAIN {label: 0}]->(k), " +
        "(h)-[:TRAIN {label: 0}]->(m), " +
        "(i)-[:TRAIN {label: 0}]->(j), " +
        "(k)-[:TRAIN {label: 0}]->(m), " +
        "(k)-[:TRAIN {label: 0}]->(o), " +
        // 5 false positive TEST rels
        "(a)-[:TEST {label: 0}]->(f), " +
        "(b)-[:TEST {label: 0}]->(f), " +
        "(i)-[:TEST {label: 0}]->(k), " +
        "(j)-[:TEST {label: 0}]->(o), " +
        "(k)-[:TEST {label: 0}]->(o)";

    @Inject
    GraphStore graphStore;

    @Inject
    Graph graph;

    @Test
    void trainsAModel() {
        var trainGraph = (CSRGraph) graphStore.getGraph(RelationshipType.of("TRAIN"), Optional.of("label"));
        var testGraph = (CSRGraph) graphStore.getGraph(RelationshipType.of("TEST"), Optional.of("label"));

        var nodeCount = 15;
        var totalPositives = 16;
        double maxNumberOfRelationships = nodeCount * (nodeCount - 1) / 2d;
        double totalNegatives = maxNumberOfRelationships - totalPositives;
        var classRatio = totalNegatives / totalPositives;

        var expectedWinner = Map.<String, Object>of("maxEpochs", 1000);
        var config = ImmutableLinkPredictionTrainConfig.builder()
            .trainRelationshipType(RelationshipType.of("TRAIN"))
            .testRelationshipType(RelationshipType.of("TEST"))
            .featureProperties(List.of("z"))
            .modelName("model")
            .validationFolds(3)
            .randomSeed(-1L)
            .negativeClassWeight(classRatio)
            .params(List.of(
                Map.of("maxEpochs", 0),
                expectedWinner
            )).build();

        var linkPredictionTrain = new LinkPredictionTrain(
            UnionGraph.of(List.of(trainGraph, testGraph)),
            config,
            TestProgressLogger.NULL_LOGGER
        );

        var model = linkPredictionTrain.compute();

        var customInfo = (LinkPredictionModelInfo) model.customInfo();
        var validationScores = customInfo.metrics().get(LinkMetric.AUCPR).validation();

        assertThat(validationScores).hasSize(2);
        var actualWinnerParams = customInfo.bestParameters();
        assertThat(actualWinnerParams).containsAllEntriesOf(expectedWinner);
        double model1Score = validationScores.get(0).avg();
        double model2Score = validationScores.get(1).avg();
        assertThat(model1Score).isNotCloseTo(model2Score, Percentage.withPercentage(0.2));
    }

    @Test
    void trainsAModelWithListFeatures() {
        var trainGraph = (CSRGraph) graphStore.getGraph(RelationshipType.of("TRAIN"), Optional.of("label"));
        var testGraph = (CSRGraph) graphStore.getGraph(RelationshipType.of("TEST"), Optional.of("label"));

        var nodeCount = 15;
        var totalPositives = 16;
        double maxNumberOfRelationships = nodeCount * (nodeCount - 1) / 2d;
        double totalNegatives = maxNumberOfRelationships - totalPositives;
        var classRatio = totalNegatives / totalPositives;

        var expectedWinner = Map.<String, Object>of("maxEpochs", 1000);
        var config = ImmutableLinkPredictionTrainConfig.builder()
            .trainRelationshipType(RelationshipType.of("TRAIN"))
            .testRelationshipType(RelationshipType.of("TEST"))
            .featureProperties(List.of("array"))
            .modelName("model")
            .validationFolds(3)
            .randomSeed(-1L)
            .negativeClassWeight(classRatio)
            .params(List.of(
                Map.of("maxEpochs", 0),
                expectedWinner
            )).build();

        var linkPredictionTrain = new LinkPredictionTrain(
            UnionGraph.of(List.of(trainGraph, testGraph)),
            config,
            TestProgressLogger.NULL_LOGGER
        );

        var model = linkPredictionTrain.compute();

        var customInfo = (LinkPredictionModelInfo) model.customInfo();
        var validationScores = customInfo.metrics().get(LinkMetric.AUCPR).validation();

        assertThat(validationScores).hasSize(2);
        var actualWinnerParams = customInfo.bestParameters();
        assertThat(actualWinnerParams).containsAllEntriesOf(expectedWinner);
        double model1Score = validationScores.get(0).avg();
        double model2Score = validationScores.get(1).avg();
        assertThat(model1Score).isNotCloseTo(model2Score, Percentage.withPercentage(0.2));
    }

    @Test
    void testLogging() {
        var trainGraph = (CSRGraph) graphStore.getGraph(RelationshipType.of("TRAIN"), Optional.of("label"));
        var testGraph = (CSRGraph) graphStore.getGraph(RelationshipType.of("TEST"), Optional.of("label"));

        var nodeCount = 15;
        var totalPositives = 16;
        double maxNumberOfRelationships = nodeCount * (nodeCount - 1) / 2d;
        double totalNegatives = maxNumberOfRelationships - totalPositives;
        var classRatio = totalNegatives / totalPositives;

        var expectedWinner = Map.<String, Object>of("maxEpochs", 10);
        var config = ImmutableLinkPredictionTrainConfig.builder()
            .trainRelationshipType(RelationshipType.of("TRAIN"))
            .testRelationshipType(RelationshipType.of("TEST"))
            .featureProperties(List.of("array"))
            .modelName("model")
            .validationFolds(3)
            .randomSeed(-1L)
            .negativeClassWeight(classRatio)
            .params(List.of(
                Map.of("maxEpochs", 0),
                expectedWinner
            )).build();

        var algo = new LinkPredictionTrainFactory(TestProgressLogger.FACTORY).build(
            graph,
            config,
            AllocationTracker.empty(),
            TestProgressLogger.NULL_LOGGER.getLog(),
            EmptyProgressEventTracker.INSTANCE
        );
        algo.compute();

        var messagesInOrder = ((TestProgressLogger) algo.getProgressLogger()).getMessages(INFO);

        AssertionsForInterfaceTypes.assertThat(messagesInOrder)
            // avoid asserting on the thread id
            .extracting(removingThreadId())
            .containsExactly(
                "LinkPredictionTrain :: Start",
                "LinkPredictionTrain :: ModelSelection :: Start",
                "LinkPredictionTrain :: ModelSelection 16%",
                "LinkPredictionTrain :: ModelSelection 33%",
                "LinkPredictionTrain :: ModelSelection 50%",
                "LinkPredictionTrain :: ModelSelection 66%",
                "LinkPredictionTrain :: ModelSelection 83%",
                "LinkPredictionTrain :: ModelSelection 100%",
                "LinkPredictionTrain :: ModelSelection :: Finished",
                "LinkPredictionTrain :: Training :: Start",
                "LinkPredictionTrain :: Training 10%",
                "LinkPredictionTrain :: Training 20%",
                "LinkPredictionTrain :: Training 30%",
                "LinkPredictionTrain :: Training 40%",
                "LinkPredictionTrain :: Training 50%",
                "LinkPredictionTrain :: Training 60%",
                "LinkPredictionTrain :: Training 70%",
                "LinkPredictionTrain :: Training 80%",
                "LinkPredictionTrain :: Training 90%",
                "LinkPredictionTrain :: Training 100%",
                "LinkPredictionTrain :: Training :: Finished",
                "LinkPredictionTrain :: Evaluation :: Start",
                "LinkPredictionTrain :: Evaluation :: Training :: Start",
                "LinkPredictionTrain :: Evaluation :: Training 100%",
                "LinkPredictionTrain :: Evaluation :: Training :: Finished",
                "LinkPredictionTrain :: Evaluation :: Testing :: Start",
                "LinkPredictionTrain :: Evaluation :: Testing 100%",
                "LinkPredictionTrain :: Evaluation :: Testing :: Finished",
                "LinkPredictionTrain :: Evaluation :: Finished",
                "LinkPredictionTrain :: Finished"
            );
    }
}

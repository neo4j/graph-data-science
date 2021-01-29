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

import org.assertj.core.data.Percentage;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.ml.linkmodels.metrics.LinkMetric;
import org.neo4j.graphalgo.Orientation;
import org.neo4j.graphalgo.RelationshipType;
import org.neo4j.graphalgo.TestProgressLogger;
import org.neo4j.graphalgo.api.GraphStore;
import org.neo4j.graphalgo.extension.GdlExtension;
import org.neo4j.graphalgo.extension.GdlGraph;
import org.neo4j.graphalgo.extension.Inject;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

@GdlExtension
class LinkPredictionTrainTest {

    @GdlGraph(orientation = Orientation.UNDIRECTED)
    static String GRAPH =
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
        "(a)-[:TEST {label: 1}]->(b)-[:TEST {label: 1}]->(c)-[:TRAIN {label: 1}]->(a), " +
        "(d)-[:TRAIN {label: 1}]->(e)-[:TRAIN {label: 1}]->(f)-[:TRAIN {label: 1}]->(d), " +
        "(g)-[:TRAIN {label: 1}]->(h)-[:TRAIN {label: 1}]->(i)-[:TRAIN {label: 1}]->(g), " +
        "(j)-[:TRAIN {label: 1}]->(k)-[:TEST {label: 1}]->(l)-[:TRAIN {label: 1}]->(j), " +
        "(m)-[:TRAIN {label: 1}]->(n)-[:TRAIN {label: 1}]->(o)-[:TRAIN {label: 1}]->(m), " +
        "(a)-[:TRAIN {label: 0}]->(d), " +
        "(b)-[:TRAIN {label: 0}]->(e), " +
        "(c)-[:TRAIN {label: 0}]->(f), " +
        "(d)-[:TRAIN {label: 0}]->(g), " +
        "(e)-[:TRAIN {label: 0}]->(h), " +
        "(f)-[:TRAIN {label: 0}]->(i), " +
        "(g)-[:TRAIN {label: 0}]->(j), " +
        "(h)-[:TRAIN {label: 0}]->(k), " +
        "(i)-[:TRAIN {label: 0}]->(l), " +
        "(j)-[:TRAIN {label: 0}]->(m), " +
        "(k)-[:TRAIN {label: 0}]->(n), " +
        "(l)-[:TRAIN {label: 0}]->(o), " +
        "(g)-[:TEST {label: 0}]->(m), " +
        "(l)-[:TEST {label: 0}]->(f), " +
        "(o)-[:TEST {label: 0}]->(a), " +
        "(k)-[:IGNORE]->(c)";

    @Inject
    GraphStore graphStore;

    @Test
    void trainsAModel() {
        var trainGraph = graphStore.getGraph(RelationshipType.of("TRAIN"), Optional.of("label"));
        var testGraph = graphStore.getGraph(RelationshipType.of("TEST"), Optional.of("label"));

        var expectedWinner = Map.<String, Object>of("maxIterations", 1000);
        var config = ImmutableLinkPredictionTrainConfig.builder()
            .trainRelationshipType(RelationshipType.of("unused"))
            .testRelationshipType(RelationshipType.of("unused"))
            .featureProperties(List.of("a"))
            .modelName("model")
            .validationFolds(3)
            .randomSeed(-1L)
            .params(List.of(
                Map.of("maxIterations", 0),
                expectedWinner
            )).build();

        var numberOfNodes = 15;
        var numberOfRelationships = 31;
        var numberOfPossibleRelationships = numberOfNodes * (numberOfNodes - 1)/ 2;
        var classRatio = (numberOfPossibleRelationships - numberOfRelationships) / (double) numberOfRelationships;

        var linkPredictionTrain = new LinkPredictionTrain(
            trainGraph,
            testGraph,
            config,
            classRatio,
            TestProgressLogger.NULL_LOGGER.getLog()
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
}

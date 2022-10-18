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
package org.neo4j.gds.ml.pipeline.linkPipeline.train;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.utils.TerminationFlag;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.ml.models.Classifier;
import org.neo4j.gds.ml.models.logisticregression.LogisticRegressionTrainConfig;
import org.neo4j.gds.ml.pipeline.linkPipeline.LinkPredictionTrainingPipeline;
import org.neo4j.gds.ml.pipeline.linkPipeline.linkfunctions.HadamardFeatureStep;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.gds.ml.metrics.LinkMetric.AUCPR;

@GdlExtension
class LinkPredictionClassImbalanceTrainTest {

    @GdlGraph(graphNamePrefix = "train", idOffset = 42, orientation = Orientation.UNDIRECTED)
    static String GRAPH =
        "(a:N {array: [0.9,1.0]}), " +
        "(b:N {array: [0.8,1.1]}), " +
        "(c:N {array: [1.5,1.0]}), " +
        "(e:N {array: [1.0,4.5]}), " +
        "(f:N {array: [2.0,5.1]}), " +
        "(g:N {array: [1.0,4.19]}), " +
        "(h:N {array: [1.4,5.5]}), " +
        "(i:N {array: [4.8,0.3]}), " +

        "(a)-[:REL {label: 1.0}]->(b), " +
        "(a)-[:REL {label: 1.0}]->(c), " +
        "(b)-[:REL {label: 1.0}]->(c), " +

        "(a)-[:REL {label: 0.0}]->(e), " +
        "(a)-[:REL {label: 0.0}]->(f), " +
        "(a)-[:REL {label: 0.0}]->(g), " +
        "(a)-[:REL {label: 0.0}]->(h), " +
        "(a)-[:REL {label: 0.0}]->(i), " +

        "(b)-[:REL {label: 0.0}]->(e), " +
        "(b)-[:REL {label: 0.0}]->(f), " +
        "(b)-[:REL {label: 0.0}]->(g), " +
        "(b)-[:REL {label: 0.0}]->(h), " +
        "(b)-[:REL {label: 0.0}]->(i), " +

        "(c)-[:REL {label: 0.0}]->(e), " +
        "(c)-[:REL {label: 0.0}]->(f), " +
        "(c)-[:REL {label: 0.0}]->(g), " +
        "(c)-[:REL {label: 0.0}]->(h), " +
        "(c)-[:REL {label: 0.0}]->(i), " +

        "(e)-[:REL {label: 0.0}]->(f), " +
        "(e)-[:REL {label: 0.0}]->(g), " +
        "(e)-[:REL {label: 0.0}]->(h), " +
        "(e)-[:REL {label: 0.0}]->(i), " +

        "(f)-[:REL {label: 0.0}]->(g), " +
        "(f)-[:REL {label: 0.0}]->(h), " +
        "(f)-[:REL {label: 0.0}]->(i), " +

        "(g)-[:REL {label: 0.0}]->(h), " +
        "(g)-[:REL {label: 0.0}]->(i), " +

        "(h)-[:REL {label: 0.0}]->(i)";

    @Inject
    Graph trainGraph;

    @Test
    void focalLossImprovesMinorityClassPredictions() {
        var linkABWithNoFocus = train(0).predictProbabilities(new double[]{0.72, 1.1});
        var linkABWithFocus = train(5).predictProbabilities(new double[]{0.72, 1.1});

        //(0.72, 1.1) is the feature vector for link [a,b].
        //[a,b] is a positive link with features x<y, but all other positive links have features with x>y. All negative links have features with x<y. Hence (a,b) is a hard-to-classify positive(minority) example.
        //Increasing focus should make it more probable to classify it as positive link. (index 1 in probabilities vector).
        assertThat(linkABWithFocus[1]).isGreaterThan(linkABWithNoFocus[1]);
    }

    private Classifier train(int focusWeight) {
        LinkPredictionTrainingPipeline pipeline = new LinkPredictionTrainingPipeline();
        pipeline.addTrainerConfig(LogisticRegressionTrainConfig.of(Map.of(
            "penalty", 0.1,
            "patience", 5,
            "tolerance", 0.001,
            "focusWeight", focusWeight
        )));
        pipeline.addFeatureStep(new HadamardFeatureStep(List.of("array")));

        return new LinkPredictionTrain(
            trainGraph,
            trainGraph,
            pipeline,
            LinkPredictionTrainConfigImpl.builder()
                .modelUser("DUMMY")
                .modelName("model")
                .graphName("g")
                .targetRelationshipType("REL")
                .sourceNodeLabel("N")
                .targetNodeLabel("N")
                .pipeline("DUMMY")
                .metrics(List.of(AUCPR))
                .negativeClassWeight(1)
                .randomSeed(1337L)
                .build(),
            ProgressTracker.NULL_TRACKER,
            TerminationFlag.RUNNING_TRUE
        ).compute().classifier();
    }
}

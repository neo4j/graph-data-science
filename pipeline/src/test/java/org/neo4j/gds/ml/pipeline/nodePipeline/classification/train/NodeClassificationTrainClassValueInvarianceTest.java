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
package org.neo4j.gds.ml.pipeline.nodePipeline.classification.train;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.ml.metrics.classification.Accuracy;
import org.neo4j.gds.ml.metrics.classification.ClassificationMetricSpecification;
import org.neo4j.gds.ml.metrics.classification.GlobalAccuracy;
import org.neo4j.gds.ml.models.logisticregression.LogisticRegressionTrainConfigImpl;
import org.neo4j.gds.ml.pipeline.nodePipeline.NodeFeatureProducer;
import org.neo4j.gds.ml.pipeline.nodePipeline.NodeFeatureStep;
import org.neo4j.gds.ml.pipeline.nodePipeline.classification.NodeClassificationTrainingPipeline;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@GdlExtension
public class NodeClassificationTrainClassValueInvarianceTest {

    private static final String GRAPH_NAME_1 = "G11";

    @GdlGraph(graphNamePrefix = "nodes1")
    private static final String DB_QUERY1 =
        "CREATE " +
        "  (a1:N {bananas: 100.0, arrayProperty: [1.2, 1.2], a: 1.2, b: 1.2, t: 0})" +
        ", (a2:N {bananas: 100.0, arrayProperty: [2.8, 2.5], a: 2.8, b: 2.5, t: 0})" +
        ", (a3:N {bananas: 100.0, arrayProperty: [3.3, 0.5], a: 3.3, b: 0.5, t: 0})" +
        ", (a4:N {bananas: 100.0, arrayProperty: [1.0, 0.5], a: 1.0, b: 0.5, t: 0})" +
        ", (a5:N {bananas: 100.0, arrayProperty: [1.32, 0.5], a: 1.32, b: 0.5, t: 0})" +
        ", (a6:N {bananas: 100.0, arrayProperty: [1.3, 1.5], a: 1.3, b: 1.5, t: 1})" +
        ", (a7:N {bananas: 100.0, arrayProperty: [5.3, 10.5], a: 5.3, b: 10.5, t: 1})" +
        ", (a8:N {bananas: 100.0, arrayProperty: [1.3, 2.5], a: 1.3, b: 2.5, t: 1})" +
        ", (a9:N {bananas: 100.0, arrayProperty: [0.0, 66.8], a: 0.0, b: 66.8, t: 1})" +
        ", (a10:N {bananas: 100.0, arrayProperty: [0.1, 2.8], a: 0.1, b: 2.8, t: 1})" +
        ", (a11:N {bananas: 100.0, arrayProperty: [0.66, 2.8], a: 0.66, b: 2.8, t: 1})" +
        ", (a12:N {bananas: 100.0, arrayProperty: [2.0, 10.8], a: 2.0, b: 10.8, t: 1})" +
        ", (a13:N {bananas: 100.0, arrayProperty: [5.0, 7.8], a: 5.0, b: 7.8, t: 2})" +
        ", (a14:N {bananas: 100.0, arrayProperty: [4.0, 5.8], a: 4.0, b: 5.8, t: 2})" +
        ", (a15:N {bananas: 100.0, arrayProperty: [1.0, 0.9], a: 1.0, b: 0.9, t: 2})";

    @Inject
    private GraphStore nodes1GraphStore;

    private static final String GRAPH_NAME_2 = "G2";

    @GdlGraph(graphNamePrefix = "nodes2")
    private static final String DB_QUERY2 =
        "CREATE " +
        "  (a1:N {bananas: 100.0, arrayProperty: [1.2, 1.2], a: 1.2, b: 1.2, t: 0})" +
        ", (a2:N {bananas: 100.0, arrayProperty: [2.8, 2.5], a: 2.8, b: 2.5, t: 0})" +
        ", (a3:N {bananas: 100.0, arrayProperty: [3.3, 0.5], a: 3.3, b: 0.5, t: 0})" +
        ", (a4:N {bananas: 100.0, arrayProperty: [1.0, 0.5], a: 1.0, b: 0.5, t: 0})" +
        ", (a5:N {bananas: 100.0, arrayProperty: [1.32, 0.5], a: 1.32, b: 0.5, t: 0})" +
        ", (a6:N {bananas: 100.0, arrayProperty: [1.3, 1.5], a: 1.3, b: 1.5, t: 222})" +
        ", (a7:N {bananas: 100.0, arrayProperty: [5.3, 10.5], a: 5.3, b: 10.5, t: 222})" +
        ", (a8:N {bananas: 100.0, arrayProperty: [1.3, 2.5], a: 1.3, b: 2.5, t: 222})" +
        ", (a9:N {bananas: 100.0, arrayProperty: [0.0, 66.8], a: 0.0, b: 66.8, t: 222})" +
        ", (a10:N {bananas: 100.0, arrayProperty: [0.1, 2.8], a: 0.1, b: 2.8, t: 222})" +
        ", (a11:N {bananas: 100.0, arrayProperty: [0.66, 2.8], a: 0.66, b: 2.8, t: 222})" +
        ", (a12:N {bananas: 100.0, arrayProperty: [2.0, 10.8], a: 2.0, b: 10.8, t: 222})" +
        ", (a13:N {bananas: 100.0, arrayProperty: [5.0, 7.8], a: 5.0, b: 7.8, t: 333})" +
        ", (a14:N {bananas: 100.0, arrayProperty: [4.0, 5.8], a: 4.0, b: 5.8, t: 333})" +
        ", (a15:N {bananas: 100.0, arrayProperty: [1.0, 0.9], a: 1.0, b: 0.9, t: 333})";

    @Inject
    private GraphStore nodes2GraphStore;

    /**
     * This tests that the specific class values do not matter, as long as the ordering is the same.
     * However, if the *ordering* of the class values differ, the splits in cross-validation could differ, resulting in different accuracies.
     */
    @Test
    void trainWithDifferentClassValues() {
        var pipeline = new NodeClassificationTrainingPipeline();
        pipeline.addFeatureStep(NodeFeatureStep.of("a"));
        pipeline.addFeatureStep(NodeFeatureStep.of("b"));

        var lrTrainerConfig = LogisticRegressionTrainConfigImpl.builder().build();
        pipeline.addTrainerConfig(lrTrainerConfig);

        var accuracyMetricSpec = ClassificationMetricSpecification.Parser.parse("accuracy");
        var accuracyPerClassMetricSpec = ClassificationMetricSpecification.Parser.parse("accuracy(class=*)");

        var config01 = createConfig("model1", GRAPH_NAME_1, List.of(accuracyMetricSpec, accuracyPerClassMetricSpec), 1L);
        var ncTrain01 = createWithExecutionContext(
            nodes1GraphStore,
            pipeline,
            config01,
            ProgressTracker.NULL_TRACKER
        );
        var result01 = ncTrain01.run();
        assertThat(result01.classifier().data().featureDimension()).isEqualTo(2);

        //Run with graph that have class values 0 and 2
        var config02 = createConfig("model2", GRAPH_NAME_2, List.of(accuracyMetricSpec, accuracyPerClassMetricSpec), 1L);
        var ncTrain02 = createWithExecutionContext(
            nodes2GraphStore,
            pipeline,
            config02,
            ProgressTracker.NULL_TRACKER
        );
        var result02 = ncTrain02.run();
        assertThat(result01.classifier().data().featureDimension()).isEqualTo(2);

        var globalAccuracy = new GlobalAccuracy();
        var accuracyForClass1 = new Accuracy(0, 0);
        var accuracyForClass2 = new Accuracy(1, 1);
        var accuracyForClass3 = new Accuracy(2, 2);

        var accuracyForClass0 = new Accuracy(0, 0);
        var accuracyForClass222 = new Accuracy(222, 1);
        var accuracyForClass333 = new Accuracy(333, 2);

        assertThat(result01.trainingStatistics().getTrainStats(globalAccuracy)).isEqualTo(result02.trainingStatistics().getTrainStats(globalAccuracy));
        assertThat(result01.trainingStatistics().getTrainStats(accuracyForClass1)).isEqualTo(result02.trainingStatistics().getTrainStats(accuracyForClass0));
        assertThat(result01.trainingStatistics().getTrainStats(accuracyForClass2)).isEqualTo(result02.trainingStatistics().getTrainStats(accuracyForClass222));
        assertThat(result01.trainingStatistics().getTrainStats(accuracyForClass3)).isEqualTo(result02.trainingStatistics().getTrainStats(accuracyForClass333));

        assertThat(result01.trainingStatistics().getValidationStats(globalAccuracy)).isEqualTo(result02.trainingStatistics().getValidationStats(globalAccuracy));
        assertThat(result01.trainingStatistics().getValidationStats(accuracyForClass1)).isEqualTo(result02.trainingStatistics().getValidationStats(accuracyForClass0));
        assertThat(result01.trainingStatistics().getValidationStats(accuracyForClass2)).isEqualTo(result02.trainingStatistics().getValidationStats(accuracyForClass222));
        assertThat(result01.trainingStatistics().getValidationStats(accuracyForClass3)).isEqualTo(result02.trainingStatistics().getValidationStats(accuracyForClass333));

        assertThat(result01.trainingStatistics().getTestScore(globalAccuracy)).isEqualTo(result02.trainingStatistics().getTestScore(globalAccuracy));
        assertThat(result01.trainingStatistics().getTestScore(accuracyForClass1)).isEqualTo(result02.trainingStatistics().getTestScore(accuracyForClass0));
        assertThat(result01.trainingStatistics().getTestScore(accuracyForClass2)).isEqualTo(result02.trainingStatistics().getTestScore(accuracyForClass222));
        assertThat(result01.trainingStatistics().getTestScore(accuracyForClass3)).isEqualTo(result02.trainingStatistics().getTestScore(accuracyForClass333));

    }

    private NodeClassificationPipelineTrainConfig createConfig(
        String modelName,
        String graphName,
        List<ClassificationMetricSpecification> metricSpecification,
        long randomSeed
    ) {
        return NodeClassificationPipelineTrainConfigImpl.builder()
            .pipeline("")
            .graphName(graphName)
            .modelUser("DUMMY")
            .modelName(modelName)
            .concurrency(1)
            .randomSeed(randomSeed)
            .targetProperty("t")
            .metrics(metricSpecification)
            .build();
    }

    static NodeClassificationTrain createWithExecutionContext(
        GraphStore graphStore,
        NodeClassificationTrainingPipeline pipeline,
        NodeClassificationPipelineTrainConfig config,
        ProgressTracker progressTracker
    ) {
        var nodeFeatureProducer = NodeFeatureProducer.create(graphStore, config, ExecutionContext.EMPTY, progressTracker);
        return NodeClassificationTrain.create(
            graphStore,
            pipeline,
            config,
            nodeFeatureProducer,
            progressTracker
        );
    }

}

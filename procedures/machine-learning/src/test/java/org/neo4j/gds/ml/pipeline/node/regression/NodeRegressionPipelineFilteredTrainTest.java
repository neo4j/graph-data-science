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
package org.neo4j.gds.ml.pipeline.node.regression;

import org.assertj.core.api.Condition;
import org.assertj.core.data.Offset;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.api.DefaultValue;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.extension.Neo4jGraph;
import org.neo4j.gds.extension.Neo4jModelCatalogExtension;
import org.neo4j.gds.ml.pipeline.PipelineCatalog;
import org.neo4j.gds.ml.pipeline.node.regression.configure.NodeRegressionPipelineAddStepProcs;
import org.neo4j.gds.ml.pipeline.node.regression.configure.NodeRegressionPipelineAddTrainerMethodProcs;
import org.neo4j.gds.ml.pipeline.node.regression.configure.NodeRegressionPipelineConfigureSplitProc;
import org.neo4j.gds.ml.pipeline.node.regression.configure.NodeRegressionPipelineCreateProc;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@Neo4jModelCatalogExtension
public class NodeRegressionPipelineFilteredTrainTest extends BaseProcTest {

    private static final String GRAPH_NAME = "g";

    @Neo4jGraph
    private static final String DB_QUERY =
        "CREATE " +
        "  (x1:X {target: 0.0})" +
        ", (x2:X {target: 1.0})" +
        ", (x3:X {target: 0.0})" +
        ", (x4:X {target: 1.0})" +
        ", (x5:X {target: 0.0})" +
        ", (x6:X {target: 1.0})" +
        ", (x7:X {target: 0.0})" +
        ", (x8:X {target: 1.0})" +
        ", (x9:X {target: 0.0})" +
        ", (x10:X {target: 1.0})" +

        ", (y1:Y {})" +
        ", (y2:Y {})" +

        ", (x2)-[:R]->(y1)" +
        ", (x4)-[:R]->(y1)" +
        ", (x6)-[:R]->(y1)" +
        ", (x8)-[:R]->(y1)" +
        ", (x10)-[:R]->(y1)" +
        ", (x2)-[:R]->(y2)" +
        ", (x4)-[:R]->(y2)" +
        ", (x6)-[:R]->(y2)" +
        ", (x8)-[:R]->(y2)" +
        ", (x10)-[:R]->(y2)";

    @BeforeEach
    void setup() throws Exception {
        registerProcedures(
            GraphProjectProc.class,
            NodeRegressionPipelineCreateProc.class,
            NodeRegressionPipelineAddStepProcs.class,
            NodeRegressionPipelineAddTrainerMethodProcs.class,
            NodeRegressionPipelineConfigureSplitProc.class,
            NodeRegressionPipelineTrainProc.class
        );

        String createQuery = GdsCypher.call(GRAPH_NAME)
            .graphProject()
            .withNodeLabels("X", "Y")
            .withRelationshipType("R")
            .withNodeProperties(List.of("target"), DefaultValue.DEFAULT)
            .yields();

        runQuery(createQuery);
    }

    @AfterEach
    void tearDown() {
        PipelineCatalog.removeAll();
    }

    @Test
    void trainWithTargetAndContextNodeLabels() {
        runQuery("CALL gds.alpha.pipeline.nodeRegression.create('p')");

        runQuery("CALL gds.alpha.pipeline.nodeRegression.addNodeProperty('p', 'degree', {" +
                 "mutateProperty: 'degree', " +
                 "contextNodeLabels: ['Y']" +
                 "})");

        runQuery("CALL gds.alpha.pipeline.nodeRegression.selectFeatures('p', 'degree')");

        runQuery("CALL gds.alpha.pipeline.nodeRegression.configureSplit('p', {" +
                 "  testFraction: 0.2, " +
                 "  validationFolds: 2" +
                 "})");

        runQuery("CALL gds.alpha.pipeline.nodeRegression.addLinearRegression('p', {penalty: 0, maxEpochs: 1000, tolerance: 0.0001, patience: 10})");
        assertCypherResult("CALL gds.alpha.pipeline.nodeRegression.train('g', {" +
                 " targetNodeLabels: ['X']," +
                 " pipeline: 'p'," +
                 " modelName: 'model'," +
                 " targetProperty: 'target'," +
                 " metrics: ['MEAN_SQUARED_ERROR']," +
                 " randomSeed: 2" +
                 "}) " +
                 "YIELD modelInfo " +
                           "RETURN" +
                           "  modelInfo.metrics.MEAN_SQUARED_ERROR.validation.avg AS avgValidScore," +
                           "  modelInfo.metrics.MEAN_SQUARED_ERROR.train.avg AS avgTrainScore," +
                           "  modelInfo.metrics.MEAN_SQUARED_ERROR.outerTrain AS outerTrainScore," +
                           "  modelInfo.metrics.MEAN_SQUARED_ERROR.test AS testScore",
            List.of(Map.of("avgTrainScore",
                closeTo(0.0), "avgValidScore", closeTo(0.0), "outerTrainScore", closeTo(0.0), "testScore", closeTo(0.0)))
        );
        //Score = 0 means the model is able to recover the perfect correlation between degree-class.
        //This implies the correct contextNodes have been used (for degree to be correct)
    }

    private Condition<Object> closeTo(double expected) {
        return new Condition<>(mss -> {
            assertThat((double) mss).isCloseTo(expected, Offset.offset(2e-2));
            return true;
        }, "a model selection statistics map");
    }
}

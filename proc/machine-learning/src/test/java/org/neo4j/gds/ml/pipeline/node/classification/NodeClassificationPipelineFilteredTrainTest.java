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
package org.neo4j.gds.ml.pipeline.node.classification;

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
import org.neo4j.gds.ml.pipeline.node.classification.predict.NodeClassificationPipelineTrainProc;

import java.util.List;
import java.util.Map;

@Neo4jModelCatalogExtension
public class NodeClassificationPipelineFilteredTrainTest extends BaseProcTest {

    private static final String GRAPH_NAME = "g";
    private static final String MODEL_NAME = "model";

    @Neo4jGraph
    private static final String DB_QUERY =
        "CREATE " +
        "  (x1:X {class: 0})" +
        ", (x2:X {class: 1})" +
        ", (x3:X {class: 0})" +
        ", (x4:X {class: 1})" +
        ", (x5:X {class: 0})" +
        ", (x6:X {class: 1})" +
        ", (x7:X {class: 0})" +
        ", (x8:X {class: 1})" +
        ", (x9:X {class: 0})" +
        ", (x10:X {class: 1})" +

        ", (y1:Y {class: 2})" +
        ", (y2:Y {class: 2})" +

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

    public static final String PIPELINE_NAME = "pipe";

    @BeforeEach
    void setup() throws Exception {
        registerProcedures(
            GraphProjectProc.class,
            NodeClassificationPipelineCreateProc.class,
            NodeClassificationPipelineAddStepProcs.class,
            NodeClassificationPipelineAddTrainerMethodProcs.class,
            NodeClassificationPipelineConfigureSplitProc.class,
            NodeClassificationPipelineTrainProc.class
        );

        String createQuery = GdsCypher.call(GRAPH_NAME)
            .graphProject()
            .withNodeLabels("X", "Y")
            .withRelationshipType("R")
            .withNodeProperties(List.of("class"), DefaultValue.DEFAULT)
            .yields();

        runQuery(createQuery);
    }

    @AfterEach
    void tearDown() {
        PipelineCatalog.removeAll();
    }

    @Test
    void trainWithTargetAndContextNodeLabels() {
        runQuery("CALL gds.beta.pipeline.nodeClassification.create('p')");

        runQuery("CALL gds.beta.pipeline.nodeClassification.addNodeProperty('p', 'degree', {" +
                 "  mutateProperty: 'degree' " +
                 "})");

        runQuery("CALL gds.beta.pipeline.nodeClassification.selectFeatures('p', 'degree')");

        runQuery("CALL gds.beta.pipeline.nodeClassification.configureSplit('p', {" +
                 "  testFraction: 0.2, " +
                 "  validationFolds: 2" +
                 "})");

        runQuery("CALL gds.beta.pipeline.nodeClassification.addLogisticRegression('p', {penalty: 0, maxEpochs: 100})");

        assertCypherResult("CALL gds.beta.pipeline.nodeClassification.train('g', {" +
                 " targetNodeLabels: ['X']," +
                 " contextNodeLabels: ['Y']," +
                 " pipeline: 'p'," +
                 " modelName: 'model'," +
                 " targetProperty: 'class'," +
                 " metrics: ['ACCURACY']," +
                 " randomSeed: 2" +
                 "}) " +
                 "YIELD modelInfo " +
                           "RETURN modelInfo.metrics.ACCURACY.train.avg AS avgTrainScore," +
                           "  modelInfo.metrics.ACCURACY.outerTrain AS outerTrainScore," +
                           "  modelInfo.metrics.ACCURACY.test AS testScore",
            List.of(Map.of("avgTrainScore", 1.0, "outerTrainScore", 1.0, "testScore", 1.0))
        );
        //Score = 1 means the model is able to recover the perfect correlation between degree-class.
        //This implies the correct contextNodes have been used (for degree to be correct)
    }
}

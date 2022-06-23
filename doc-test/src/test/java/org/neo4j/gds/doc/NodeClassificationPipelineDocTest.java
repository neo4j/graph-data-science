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
package org.neo4j.gds.doc;

import org.junit.jupiter.api.AfterAll;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.catalog.GraphStreamNodePropertiesProc;
import org.neo4j.gds.functions.AsNodeFunc;
import org.neo4j.gds.ml.pipeline.PipelineCatalog;
import org.neo4j.gds.ml.pipeline.node.classification.NodeClassificationPipelineAddStepProcs;
import org.neo4j.gds.ml.pipeline.node.classification.NodeClassificationPipelineAddTrainerMethodProcs;
import org.neo4j.gds.ml.pipeline.node.classification.NodeClassificationPipelineConfigureAutoTuningProc;
import org.neo4j.gds.ml.pipeline.node.classification.NodeClassificationPipelineConfigureSplitProc;
import org.neo4j.gds.ml.pipeline.node.classification.NodeClassificationPipelineCreateProc;
import org.neo4j.gds.ml.pipeline.node.classification.predict.NodeClassificationPipelineMutateProc;
import org.neo4j.gds.ml.pipeline.node.classification.predict.NodeClassificationPipelineStreamProc;
import org.neo4j.gds.ml.pipeline.node.classification.predict.NodeClassificationPipelineTrainProc;
import org.neo4j.gds.ml.pipeline.node.classification.predict.NodeClassificationPipelineWriteProc;
import org.neo4j.gds.scaling.ScalePropertiesMutateProc;

import java.util.List;

class NodeClassificationPipelineDocTest extends MultiFileDocTestBase {

    @AfterAll
    static void tearDown() {
        PipelineCatalog.removeAll();
    }

    @Override
    protected List<Class<?>> functions() {
        return List.of(AsNodeFunc.class);
    }

    @Override
    protected List<Class<?>> procedures() {
        return List.of(
            NodeClassificationPipelineCreateProc.class,
            NodeClassificationPipelineMutateProc.class,
            NodeClassificationPipelineWriteProc.class,
            NodeClassificationPipelineStreamProc.class,
            NodeClassificationPipelineTrainProc.class,
            NodeClassificationPipelineAddStepProcs.class,
            NodeClassificationPipelineConfigureSplitProc.class,
            NodeClassificationPipelineConfigureAutoTuningProc.class,
            NodeClassificationPipelineAddTrainerMethodProcs.class,
            GraphStreamNodePropertiesProc.class,
            GraphProjectProc.class,
            ScalePropertiesMutateProc.class
        );
    }

    @Override
    protected List<String> adocPaths() {
        return List.of(
            "pages/machine-learning/node-property-prediction/nodeclassification-pipelines/config.adoc",
            "pages/machine-learning/node-property-prediction/nodeclassification-pipelines/training.adoc",
            "pages/machine-learning/node-property-prediction/nodeclassification-pipelines/predict.adoc"
        );
    }
}

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
package org.neo4j.gds.ml.pipeline.node.regression.configure;

import org.junit.jupiter.api.AfterEach;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.ml.pipeline.PipelineCatalog;
import org.neo4j.gds.ml_api.TrainingMethod;

import java.util.List;
import java.util.Map;

abstract class NodeRegressionPipelineBaseProcTest extends BaseProcTest {

    static final Map<String, List<Object>> DEFAULT_PARAMETERSPACE = Map.of(
        TrainingMethod.LinearRegression.toString(), List.of(),
        TrainingMethod.RandomForestRegression.toString(), List.of()
    );

    @AfterEach
    void tearDown() {
        PipelineCatalog.removeAll();
    }
}

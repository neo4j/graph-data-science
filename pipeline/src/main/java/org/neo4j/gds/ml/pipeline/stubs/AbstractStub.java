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
package org.neo4j.gds.ml.pipeline.stubs;

import org.neo4j.gds.mem.MemoryEstimation;
import org.neo4j.gds.exceptions.MemoryEstimationNotImplementedException;
import org.neo4j.gds.ml.pipeline.Stub;
import org.neo4j.gds.procedures.algorithms.AlgorithmsProcedureFacade;
import org.neo4j.gds.procedures.algorithms.stubs.MutateStub;

import java.util.Map;

/**
 * Just some structural reuse. One case where reuse-via-inheritance makes sense.
 * Look at how regular the implementations are: can prolly be generated.
 */
abstract class AbstractStub<CONFIGURATION, RESULT> implements Stub {
    @Override
    public void validateBeforeCreatingNodePropertyStep(
        AlgorithmsProcedureFacade facade,
        Map<String, Object> configuration
    ) {
        stub(facade).validateConfiguration(configuration);
    }

    @Override
    public MemoryEstimation getMemoryEstimation(
        AlgorithmsProcedureFacade facade,
        String username,
        Map<String, Object> configuration
    ) throws MemoryEstimationNotImplementedException {
        return stub(facade).getMemoryEstimation(username, configuration);
    }

    @Override
    public void execute(
        AlgorithmsProcedureFacade facade,
        String graphName,
        Map<String, Object> configuration
    ) {
        stub(facade).execute(graphName, configuration);
    }

    protected abstract MutateStub<CONFIGURATION, RESULT> stub(AlgorithmsProcedureFacade facade);
}

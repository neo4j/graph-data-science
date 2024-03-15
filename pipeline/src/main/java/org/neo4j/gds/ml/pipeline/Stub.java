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
package org.neo4j.gds.ml.pipeline;


import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.exceptions.MemoryEstimationNotImplementedException;
import org.neo4j.gds.procedures.algorithms.AlgorithmsProcedureFacade;

import java.util.Map;

public interface Stub {
    /**
     * Validate user input, but not in relation to the user. This translates to validating with global defaults and limits only.
     */
    void validateBeforeCreatingNodePropertyStep(AlgorithmsProcedureFacade facade, Map<String, Object> configuration);

    /**
     * Do the estimate. Note that here we do not apply defaults and limits when parsing configuration.
     *
     * @param configuration it is a little bit enhanced and should not be modified
     * @throws MemoryEstimationNotImplementedException this is valid in some cases, caller can handle
     */
    MemoryEstimation estimate(AlgorithmsProcedureFacade facade, String username, Map<String, Object> configuration) throws MemoryEstimationNotImplementedException;

    void execute(AlgorithmsProcedureFacade algorithmsProcedureFacade, String graphName, Map<String, Object> configuration);
}

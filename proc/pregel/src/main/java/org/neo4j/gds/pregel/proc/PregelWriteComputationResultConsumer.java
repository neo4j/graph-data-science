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
package org.neo4j.gds.pregel.proc;

import org.neo4j.gds.Algorithm;
import org.neo4j.gds.WriteNodePropertiesComputationResultConsumer;
import org.neo4j.gds.beta.pregel.PregelProcedureConfig;
import org.neo4j.gds.beta.pregel.PregelResult;

public abstract class PregelWriteComputationResultConsumer<
    ALGO extends Algorithm<PregelResult>,
    CONFIG extends PregelProcedureConfig
  > extends WriteNodePropertiesComputationResultConsumer<ALGO, PregelResult, CONFIG, PregelWriteResult>
{

    public PregelWriteComputationResultConsumer() {
        super(
            (computationResult, executionContext) -> {
                var ranIterations = computationResult.result().map(PregelResult::ranIterations).orElse(0);
                var didConverge = computationResult.result().map(PregelResult::didConverge).orElse(false);
                return new PregelWriteResult.Builder().withRanIterations(ranIterations).didConverge(didConverge);
            },
            (computationResult) -> PregelBaseProc.nodeProperties(computationResult, computationResult.config().writeProperty()),
            "PregelWrite"
        );
    }
}

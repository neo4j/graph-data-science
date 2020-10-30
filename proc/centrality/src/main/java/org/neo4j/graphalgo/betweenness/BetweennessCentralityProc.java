/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
package org.neo4j.graphalgo.betweenness;

import org.neo4j.graphalgo.AlgoBaseProc;
import org.neo4j.graphalgo.AlgorithmFactory;
import org.neo4j.graphalgo.api.NodeProperties;
import org.neo4j.graphalgo.core.utils.paged.HugeAtomicDoubleArray;
import org.neo4j.graphalgo.result.AbstractCentralityResultBuilder;
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;

final class BetweennessCentralityProc {

    static final String BETWEENNESS_DESCRIPTION = "Betweenness centrality measures the relative information flow that passes through a node.";

    private BetweennessCentralityProc() {}

    static <CONFIG extends BetweennessCentralityBaseConfig> NodeProperties nodeProperties(AlgoBaseProc.ComputationResult<BetweennessCentrality, HugeAtomicDoubleArray, CONFIG> computeResult) {
        return computeResult.result().asNodeProperties();
    }

    static <CONFIG extends BetweennessCentralityBaseConfig> AlgorithmFactory<BetweennessCentrality, CONFIG> algorithmFactory() {
        return new BetweennessCentralityFactory<>();
    }

    static <PROC_RESULT, CONFIG extends BetweennessCentralityBaseConfig> AbstractCentralityResultBuilder<PROC_RESULT> resultBuilder(
        BetweennessCentralityResultBuilder<PROC_RESULT> procResultBuilder,
        AlgoBaseProc.ComputationResult<BetweennessCentrality, HugeAtomicDoubleArray, CONFIG> computeResult
    ) {
        return procResultBuilder.withCentralityFunction(computeResult.result() != null ? computeResult.result()::get : null);
    }

    abstract static class BetweennessCentralityResultBuilder<PROC_RESULT> extends AbstractCentralityResultBuilder<PROC_RESULT> {
        BetweennessCentralityResultBuilder(ProcedureCallContext callContext, int concurrency) {
            super(callContext, concurrency);
        }
    }
}

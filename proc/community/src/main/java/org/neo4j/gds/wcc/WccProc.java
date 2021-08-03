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
package org.neo4j.gds.wcc;

import org.neo4j.gds.AlgoBaseProc;
import org.neo4j.gds.AlgorithmFactory;
import org.neo4j.gds.CommunityProcCompanion;
import org.neo4j.gds.result.AbstractCommunityResultBuilder;
import org.neo4j.gds.api.NodeProperties;
import org.neo4j.gds.core.utils.mem.AllocationTracker;
import org.neo4j.gds.core.utils.paged.dss.DisjointSetStruct;

final class WccProc {

    static final String WCC_DESCRIPTION =
        "The WCC algorithm finds sets of connected nodes in an undirected graph, where all nodes in the same set form a connected component.";

    private WccProc() {}

    static <CONFIG extends WccBaseConfig> AlgorithmFactory<Wcc, CONFIG> algorithmFactory() {
        return new WccAlgorithmFactory<>();
    }

    static <PROC_RESULT, CONFIG extends WccBaseConfig> AbstractCommunityResultBuilder<PROC_RESULT> resultBuilder(
        AbstractCommunityResultBuilder<PROC_RESULT> procResultBuilder,
        AlgoBaseProc.ComputationResult<Wcc, DisjointSetStruct, CONFIG> computationResult
    ) {
        return procResultBuilder.withCommunityFunction(!computationResult.isGraphEmpty() ? computationResult.result()::setIdOf : null);
    }

    static <CONFIG extends WccBaseConfig> NodeProperties nodeProperties(
        AlgoBaseProc.ComputationResult<Wcc, DisjointSetStruct, CONFIG> computationResult,
        String resultProperty,
        AllocationTracker tracker
    ) {
        return CommunityProcCompanion.nodeProperties(
            computationResult,
            resultProperty,
            computationResult.result().asNodeProperties(),
            tracker
        );
    }
}

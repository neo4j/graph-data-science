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
package org.neo4j.gds.approxmaxkcut;

import org.neo4j.gds.AlgoBaseProc;
import org.neo4j.gds.AlgorithmFactory;
import org.neo4j.gds.api.NodeProperties;
import org.neo4j.gds.impl.approxmaxkcut.ApproxMaxKCut;
import org.neo4j.gds.impl.approxmaxkcut.ApproxMaxKCutConfig;
import org.neo4j.gds.impl.approxmaxkcut.ApproxMaxKCutFactory;

public final class ApproxMaxKCutProc {

    static final String APPROX_MAX_K_CUT_DESCRIPTION = "Approximate Maximum k-cut maps each node into one of k disjoint communities trying to maximize the sum of weights of relationships between these communities.";

    private ApproxMaxKCutProc() {}

    static <CONFIG extends ApproxMaxKCutConfig> AlgorithmFactory<ApproxMaxKCut, CONFIG> algorithmFactory() {
        return new ApproxMaxKCutFactory<>();
    }

    static <CONFIG extends ApproxMaxKCutConfig> NodeProperties nodeProperties(
        AlgoBaseProc.ComputationResult<ApproxMaxKCut, ApproxMaxKCut.CutResult, CONFIG> computationResult
    ) {
        return computationResult.result().asNodeProperties();
    }
}

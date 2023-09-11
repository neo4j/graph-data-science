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
package org.neo4j.gds.k1coloring;

import org.neo4j.gds.api.ProcedureReturnColumns;
import org.neo4j.gds.executor.ComputationResult;
import org.neo4j.gds.result.AbstractResultBuilder;

final class K1ColoringSpecificationHelper {
    static final String K1_COLORING_DESCRIPTION = "The K-1 Coloring algorithm assigns a color to every node in the graph.";
    private static final String COLOR_COUNT_FIELD_NAME = "colorCount";

    private K1ColoringSpecificationHelper() {}

    static <PROC_RESULT, CONFIG extends K1ColoringBaseConfig> AbstractResultBuilder<PROC_RESULT> resultBuilder(
        K1ColoringResultBuilder<PROC_RESULT> procResultBuilder,
        ComputationResult<K1Coloring, K1ColoringResult, CONFIG> computeResult,
        ProcedureReturnColumns returnColumns
    ) {

        var result= computeResult.result().orElse(null);
        if (returnColumns.contains(COLOR_COUNT_FIELD_NAME)) {
            procResultBuilder.withColorCount(computeResult.isGraphEmpty() ? 0 : result.usedColors().cardinality());
        }

        return procResultBuilder
            .withRanIterations(computeResult.isGraphEmpty() ? 0 : result.ranIterations())
            .withDidConverge(computeResult.isGraphEmpty() ? false : result.didConverge());
    }

}

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
package org.neo4j.gds.beta.k1coloring;

import org.neo4j.gds.CommunityProcCompanion;
import org.neo4j.gds.api.ProcedureReturnColumns;
import org.neo4j.gds.api.properties.nodes.EmptyLongNodePropertyValues;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.executor.ComputationResult;
import org.neo4j.gds.result.AbstractCommunityResultBuilder;
import org.neo4j.gds.result.AbstractResultBuilder;

final class K1ColoringProc {
    static final String K1_COLORING_DESCRIPTION = "The K-1 Coloring algorithm assigns a color to every node in the graph.";
    private static final String COLOR_COUNT_FIELD_NAME = "colorCount";

    private K1ColoringProc() {}

    static <PROC_RESULT, CONFIG extends K1ColoringConfig> AbstractResultBuilder<PROC_RESULT> resultBuilder(
        K1ColoringResultBuilder<PROC_RESULT> procResultBuilder,
        ComputationResult<K1Coloring, HugeLongArray, CONFIG> computeResult,
        ProcedureReturnColumns returnColumns
    ) {
        if (returnColumns.contains(COLOR_COUNT_FIELD_NAME)) {
            procResultBuilder.withColorCount(computeResult.algorithm().usedColors().cardinality());
        }

        return procResultBuilder
            .withRanIterations(computeResult.isGraphEmpty() ? 0 : computeResult.algorithm().ranIterations())
            .withDidConverge(computeResult.isGraphEmpty() ? false : computeResult.algorithm().didConverge());
    }

    static <CONFIG extends K1ColoringConfig> NodePropertyValues nodeProperties(ComputationResult<K1Coloring, HugeLongArray, CONFIG> computeResult) {
        var config = computeResult.config();
        return CommunityProcCompanion.considerSizeFilter(
            config,
            computeResult.result()
                .map(HugeLongArray::asNodeProperties)
                .orElse(EmptyLongNodePropertyValues.INSTANCE)
        );
    }

    abstract static class K1ColoringResultBuilder<PROC_RESULT> extends AbstractCommunityResultBuilder<PROC_RESULT> {
        long colorCount = -1L;
        long ranIterations;
        boolean didConverge;

        K1ColoringResultBuilder(
            ProcedureReturnColumns returnColumns,
            int concurrency
        ) {
            super(returnColumns, concurrency);
        }

        K1ColoringResultBuilder<PROC_RESULT> withColorCount(long colorCount) {
            this.colorCount = colorCount;
            return this;
        }

        K1ColoringResultBuilder<PROC_RESULT> withRanIterations(long ranIterations) {
            this.ranIterations = ranIterations;
            return this;
        }

        K1ColoringResultBuilder<PROC_RESULT> withDidConverge(boolean didConverge) {
            this.didConverge = didConverge;
            return this;
        }
    }
}

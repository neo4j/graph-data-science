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
package org.neo4j.graphalgo.beta.k1coloring;

import org.neo4j.graphalgo.AlgoBaseProc;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeLongArray;
import org.neo4j.graphalgo.core.write.PropertyTranslator;
import org.neo4j.graphalgo.result.AbstractCommunityResultBuilder;
import org.neo4j.graphalgo.result.AbstractResultBuilder;
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;

final class K1ColoringProc {
    static final String K1_COLORING_DESCRIPTION = "The K-1 Coloring algorithm assigns a color to every node in the graph.";
    private static final String COLOR_COUNT_FIELD_NAME = "colorCount";

    private K1ColoringProc() {}

    static <PROC_RESULT, CONFIG extends K1ColoringConfig> AbstractResultBuilder<PROC_RESULT> resultBuilder(
        K1ColoringResultBuilder<PROC_RESULT> procResultBuilder,
        AlgoBaseProc.ComputationResult<K1Coloring, HugeLongArray, CONFIG> computeResult,
        ProcedureCallContext callContext
    ) {
        if (callContext.outputFields().anyMatch((field) -> field.equals(COLOR_COUNT_FIELD_NAME))) {
            procResultBuilder.withColorCount(computeResult.algorithm().usedColors().cardinality());
        }

        return procResultBuilder
            .withRanIterations(computeResult.algorithm().ranIterations())
            .withDidConverge(computeResult.algorithm().didConverge());
    }

    static PropertyTranslator<HugeLongArray> nodePropertyTranslator() {
        return HugeLongArray.Translator.INSTANCE;
    }

    abstract static class K1ColoringResultBuilder<PROC_RESULT> extends AbstractCommunityResultBuilder<PROC_RESULT> {
        long colorCount = -1L;
        long ranIterations;
        boolean didConverge;

        K1ColoringResultBuilder(
            ProcedureCallContext callContext,
            AllocationTracker tracker
        ) {
            super(callContext, tracker);
        }

        K1ColoringResultBuilder withColorCount(long colorCount) {
            this.colorCount = colorCount;
            return this;
        }

        K1ColoringResultBuilder withRanIterations(long ranIterations) {
            this.ranIterations = ranIterations;
            return this;
        }

        K1ColoringResultBuilder withDidConverge(boolean didConverge) {
            this.didConverge = didConverge;
            return this;
        }
    }
}


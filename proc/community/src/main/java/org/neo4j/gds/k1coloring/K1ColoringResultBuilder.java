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
import org.neo4j.gds.result.AbstractCommunityResultBuilder;

abstract class K1ColoringResultBuilder<PROC_RESULT> extends AbstractCommunityResultBuilder<PROC_RESULT> {
    long colorCount = -1L;
    long ranIterations;
    boolean didConverge;

    K1ColoringResultBuilder(ProcedureReturnColumns returnColumns, int concurrency) {
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

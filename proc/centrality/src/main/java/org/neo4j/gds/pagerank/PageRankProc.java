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
package org.neo4j.gds.pagerank;

import org.neo4j.gds.api.ProcedureReturnColumns;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.core.utils.paged.HugeDoubleArray;
import org.neo4j.gds.executor.ComputationResult;
import org.neo4j.gds.result.AbstractCentralityResultBuilder;

final class PageRankProc {

    static final String PAGE_RANK_DESCRIPTION =
        "Page Rank is an algorithm that measures the transitive influence or connectivity of nodes.";

    static final String ARTICLE_RANK_DESCRIPTION =
        "Article Rank is a variant of the Page Rank algorithm, which " +
        "measures the transitive influence or connectivity of nodes.";

    static final String EIGENVECTOR_DESCRIPTION =
        "Eigenvector Centrality is an algorithm that measures the transitive influence or connectivity of nodes.";

    private PageRankProc() {}

    static <PROC_RESULT, CONFIG extends PageRankConfig> PageRankResultBuilder<PROC_RESULT> resultBuilder(
        PageRankResultBuilder<PROC_RESULT> procResultBuilder,
        ComputationResult<PageRankAlgorithm, PageRankResult, CONFIG> computeResult
    ) {
        computeResult.result().ifPresent(result -> {
            procResultBuilder
                .withDidConverge(result.didConverge())
                .withRanIterations(result.iterations())
                .withCentralityFunction(result.scores()::get)
                .withScalerVariant(computeResult.config().scaler());
        });

        return procResultBuilder;
    }

    static <CONFIG extends PageRankConfig> NodePropertyValues nodeProperties(ComputationResult<PageRankAlgorithm, PageRankResult, CONFIG> computeResult) {
        return computeResult.result()
            .map(PageRankResult::scores)
            .orElseGet(() -> HugeDoubleArray.newArray(0))
            .asNodeProperties();
    }

    abstract static class PageRankResultBuilder<PROC_RESULT> extends AbstractCentralityResultBuilder<PROC_RESULT> {
        protected long ranIterations;

        protected boolean didConverge;

        PageRankResultBuilder(ProcedureReturnColumns returnColumns, int concurrency) {
            super(returnColumns, concurrency);
        }

        PageRankResultBuilder<PROC_RESULT> withRanIterations(long ranIterations) {
            this.ranIterations = ranIterations;
            return this;
        }

        PageRankResultBuilder<PROC_RESULT> withDidConverge(boolean didConverge) {
            this.didConverge = didConverge;
            return this;
        }
    }
}

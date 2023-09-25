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
package org.neo4j.gds.triangle;

import org.neo4j.gds.api.ProcedureReturnColumns;
import org.neo4j.gds.api.properties.nodes.EmptyDoubleNodePropertyValues;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.api.properties.nodes.NodePropertyValuesAdapter;
import org.neo4j.gds.collections.ha.HugeDoubleArray;
import org.neo4j.gds.executor.ComputationResult;
import org.neo4j.gds.result.AbstractCommunityResultBuilder;
import org.neo4j.gds.result.AbstractResultBuilder;

final class LocalClusteringCoefficientCompanion {

    static final String DESCRIPTION = "The local clustering coefficient is a metric quantifying how connected the neighborhood of a node is.";


    private LocalClusteringCoefficientCompanion() {}

    static <CONFIG extends LocalClusteringCoefficientBaseConfig> NodePropertyValues nodeProperties(
        ComputationResult<LocalClusteringCoefficient, LocalClusteringCoefficientResult, CONFIG> computeResult
    ) {
        return computeResult.result()
            .map(LocalClusteringCoefficientResult::localClusteringCoefficients)
            .map(NodePropertyValuesAdapter::adapt)
            .orElse(EmptyDoubleNodePropertyValues.INSTANCE);
    }

    static <PROC_RESULT, CONFIG extends LocalClusteringCoefficientBaseConfig> AbstractResultBuilder<PROC_RESULT> resultBuilder(
        ResultBuilder<PROC_RESULT> procResultBuilder,
        ComputationResult<LocalClusteringCoefficient, LocalClusteringCoefficientResult, CONFIG> computeResult
    ) {
        var result = computeResult.result().orElse(EmptyResult.EMPTY_RESULT);

        return procResultBuilder
            .withAverageClusteringCoefficient(result.averageClusteringCoefficient());
    }

    abstract static class ResultBuilder<PROC_RESULT> extends AbstractCommunityResultBuilder<PROC_RESULT> {

        double averageClusteringCoefficient = 0;

        ResultBuilder(ProcedureReturnColumns returnColumns, int concurrency) {
            super(returnColumns, concurrency);
        }

        ResultBuilder<PROC_RESULT> withAverageClusteringCoefficient(double averageClusteringCoefficient) {
            this.averageClusteringCoefficient = averageClusteringCoefficient;
            return this;
        }
    }

    private static final class EmptyResult implements LocalClusteringCoefficientResult {

        static final EmptyResult EMPTY_RESULT = new EmptyResult();

        private EmptyResult() {}

        @Override
        public HugeDoubleArray localClusteringCoefficients() {
            return HugeDoubleArray.newArray(0);
        }

        @Override
        public double averageClusteringCoefficient() {
            return 0;
        }
    }

}

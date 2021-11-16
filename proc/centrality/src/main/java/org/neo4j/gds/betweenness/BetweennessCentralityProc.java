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
package org.neo4j.gds.betweenness;

import org.neo4j.gds.AlgoBaseProc;
import org.neo4j.gds.AlgorithmFactory;
import org.neo4j.gds.api.NodeProperties;
import org.neo4j.gds.core.utils.paged.HugeAtomicDoubleArray;
import org.neo4j.gds.result.AbstractCentralityResultBuilder;
import org.neo4j.gds.validation.BeforeLoadValidation;
import org.neo4j.gds.validation.GraphCreateConfigValidations;
import org.neo4j.gds.validation.ValidationConfiguration;
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;

import java.util.List;
import java.util.Locale;

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
        if (computeResult.result() != null) {
            HugeAtomicDoubleArray centrality = computeResult.result();

            if (procResultBuilder.computeDeprecatedStats) {
                double min = Double.MAX_VALUE;
                double max = -Double.MAX_VALUE;
                double sum = 0.0;
                for (long i = centrality.size() - 1; i >= 0; i--) {
                    double c = centrality.get(i);
                    if (c < min) {
                        min = c;
                    }
                    if (c > max) {
                        max = c;
                    }
                    sum += c;
                }

                procResultBuilder
                    .minCentrality(min)
                    .maxCentrality(max)
                    .sumCentrality(sum);
            }

            procResultBuilder.withCentralityFunction(computeResult.result()::get);
        }
        return procResultBuilder;
    }

    static <CONFIG extends BetweennessCentralityBaseConfig> ValidationConfiguration<CONFIG> getValidationConfig() {
        return new ValidationConfiguration<>() {
            @Override
            public List<BeforeLoadValidation<CONFIG>> beforeLoadValidations() {
                return List.of(
                    new GraphCreateConfigValidations.OrientationValidation<>()
                );
            }
        };
    }

    abstract static class BetweennessCentralityResultBuilder<PROC_RESULT> extends AbstractCentralityResultBuilder<PROC_RESULT> {
        double minCentrality = -1;
        double maxCentrality = -1;
        double sumCentrality = -1;
        boolean computeDeprecatedStats;

        BetweennessCentralityResultBuilder(ProcedureCallContext callContext, int concurrency) {
            super(callContext, concurrency);
            this.computeDeprecatedStats = callContext
                .outputFields()
                .anyMatch(s -> s.toLowerCase(Locale.ENGLISH).contains("score"));
        }

        BetweennessCentralityResultBuilder<PROC_RESULT> minCentrality(double minCentrality) {
            this.minCentrality = minCentrality;
            return this;
        }

        BetweennessCentralityResultBuilder<PROC_RESULT> maxCentrality(double maxCentrality) {
            this.maxCentrality = maxCentrality;
            return this;
        }

        BetweennessCentralityResultBuilder<PROC_RESULT> sumCentrality(double sumCentrality) {
            this.sumCentrality = sumCentrality;
            return this;
        }
    }
}

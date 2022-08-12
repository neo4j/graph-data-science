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

import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.config.GraphProjectConfig;
import org.neo4j.gds.config.GraphProjectFromStoreConfig;
import org.neo4j.gds.core.utils.paged.HugeDoubleArray;
import org.neo4j.gds.executor.ComputationResult;
import org.neo4j.gds.executor.validation.BeforeLoadValidation;
import org.neo4j.gds.executor.validation.ValidationConfiguration;
import org.neo4j.gds.result.AbstractCommunityResultBuilder;
import org.neo4j.gds.result.AbstractResultBuilder;
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;
import org.neo4j.logging.Log;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.neo4j.gds.ElementProjection.PROJECT_ALL;

final class LocalClusteringCoefficientCompanion {

    static final String DESCRIPTION = "The local clustering coefficient is a metric quantifying how connected the neighborhood of a node is.";


    private LocalClusteringCoefficientCompanion() {}

    static <CONFIG extends LocalClusteringCoefficientBaseConfig> NodePropertyValues nodeProperties(
        ComputationResult<LocalClusteringCoefficient, LocalClusteringCoefficient.Result, CONFIG> computeResult
    ) {
        return computeResult.result().asNodeProperties();
    }

    static <PROC_RESULT, CONFIG extends LocalClusteringCoefficientBaseConfig> AbstractResultBuilder<PROC_RESULT> resultBuilder(
        ResultBuilder<PROC_RESULT> procResultBuilder,
        ComputationResult<LocalClusteringCoefficient, LocalClusteringCoefficient.Result, CONFIG> computeResult
    ) {
        var result = Optional.ofNullable(computeResult.result())
            .orElse(EmptyResult.EMPTY_RESULT);

        return procResultBuilder
            .withAverageClusteringCoefficient(result.averageClusteringCoefficient());
    }

    static <CONFIG extends LocalClusteringCoefficientBaseConfig> ValidationConfiguration<CONFIG> getValidationConfig(Log log) {
        return new ValidationConfiguration<>() {
            @Override
            public List<BeforeLoadValidation<CONFIG>> beforeLoadValidations() {
                return List.of(
                    new WarnOnGraphsWithParallelRelationships<>(log)
                );
            }
        };
    }

    abstract static class ResultBuilder<PROC_RESULT> extends AbstractCommunityResultBuilder<PROC_RESULT> {

        double averageClusteringCoefficient = 0;

        ResultBuilder(ProcedureCallContext callContext, int concurrency) {
            super(callContext, concurrency);
        }

        ResultBuilder<PROC_RESULT> withAverageClusteringCoefficient(double averageClusteringCoefficient) {
            this.averageClusteringCoefficient = averageClusteringCoefficient;
            return this;
        }
    }

    private static final class EmptyResult implements LocalClusteringCoefficient.Result {

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

    private static final class WarnOnGraphsWithParallelRelationships<CONFIG extends LocalClusteringCoefficientBaseConfig> implements BeforeLoadValidation<CONFIG> {
        private final Log log;

        private WarnOnGraphsWithParallelRelationships(Log log) {
            this.log = log;
        }

        @Override
        public void validateConfigsBeforeLoad(GraphProjectConfig graphProjectConfig, CONFIG config) {
            if (graphProjectConfig instanceof GraphProjectFromStoreConfig) {
                GraphProjectFromStoreConfig storeConfig = (GraphProjectFromStoreConfig) graphProjectConfig;
                storeConfig.relationshipProjections().projections().entrySet().stream()
                    .filter(entry -> config.relationshipTypes().equals(Collections.singletonList(PROJECT_ALL)) ||
                                     config.relationshipTypes().contains(entry.getKey().name()))
                    .filter(entry -> entry.getValue().isMultiGraph())
                    .forEach(entry -> log.warn(
                        "Procedure runs optimal with relationship aggregation." +
                        " Projection for `%s` does not aggregate relationships." +
                        " You might experience a slowdown in the procedure execution.",
                        entry.getKey().equals(RelationshipType.ALL_RELATIONSHIPS) ? "*" : entry.getKey().name
                    ));
            }
        }
    }
}

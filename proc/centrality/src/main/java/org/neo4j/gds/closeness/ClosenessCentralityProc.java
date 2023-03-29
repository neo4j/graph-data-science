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
package org.neo4j.gds.closeness;

import org.neo4j.gds.GraphAlgorithmFactory;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.beta.closeness.ClosenessCentrality;
import org.neo4j.gds.beta.closeness.ClosenessCentralityConfig;
import org.neo4j.gds.beta.closeness.ClosenessCentralityFactory;
import org.neo4j.gds.beta.closeness.ClosenessCentralityResult;
import org.neo4j.gds.executor.ComputationResult;
import org.neo4j.gds.executor.validation.BeforeLoadValidation;
import org.neo4j.gds.executor.validation.GraphProjectConfigValidations;
import org.neo4j.gds.executor.validation.ValidationConfiguration;
import org.neo4j.gds.result.AbstractCentralityResultBuilder;

import java.util.List;

public final class ClosenessCentralityProc {


    private ClosenessCentralityProc() {}

    static <CONFIG extends ClosenessCentralityConfig> NodePropertyValues nodeProperties(ComputationResult<ClosenessCentrality, ClosenessCentralityResult, CONFIG> computeResult) {
        return computeResult.result().centralities().asNodeProperties();
    }

    static <CONFIG extends ClosenessCentralityConfig> GraphAlgorithmFactory<ClosenessCentrality, CONFIG> algorithmFactory() {
        return new ClosenessCentralityFactory<>();
    }

    static <PROC_RESULT, CONFIG extends ClosenessCentralityConfig> AbstractCentralityResultBuilder<PROC_RESULT> resultBuilder(
        AbstractCentralityResultBuilder<PROC_RESULT> procResultBuilder,
        ComputationResult<ClosenessCentrality, ClosenessCentralityResult, CONFIG> computeResult
    ) {
        if (computeResult.result() != null) {
            var centralities = computeResult.result().centralities();
            procResultBuilder.withCentralityFunction(centralities::get);
        }
        return procResultBuilder;
    }

    static <CONFIG extends ClosenessCentralityConfig> ValidationConfiguration<CONFIG> getValidationConfig() {
        return new ValidationConfiguration<>() {
            @Override
            public List<BeforeLoadValidation<CONFIG>> beforeLoadValidations() {
                return List.of(
                    new GraphProjectConfigValidations.OrientationValidation<>()
                );
            }
        };
    }


}

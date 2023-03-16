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

import org.neo4j.gds.GraphAlgorithmFactory;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.core.utils.paged.HugeAtomicDoubleArray;
import org.neo4j.gds.executor.ComputationResult;
import org.neo4j.gds.executor.validation.BeforeLoadValidation;
import org.neo4j.gds.executor.validation.GraphProjectConfigValidations;
import org.neo4j.gds.executor.validation.ValidationConfiguration;
import org.neo4j.gds.result.AbstractCentralityResultBuilder;

import java.util.List;

final class BetweennessCentralityProc {

    private BetweennessCentralityProc() {}

    static <CONFIG extends BetweennessCentralityBaseConfig> NodePropertyValues nodeProperties(ComputationResult<BetweennessCentrality, HugeAtomicDoubleArray, CONFIG> computeResult) {
        return computeResult.result().asNodeProperties();
    }

    static <CONFIG extends BetweennessCentralityBaseConfig> GraphAlgorithmFactory<BetweennessCentrality, CONFIG> algorithmFactory() {
        return new BetweennessCentralityFactory<>();
    }

    static <PROC_RESULT, CONFIG extends BetweennessCentralityBaseConfig> AbstractCentralityResultBuilder<PROC_RESULT> resultBuilder(
        AbstractCentralityResultBuilder<PROC_RESULT> procResultBuilder,
        ComputationResult<BetweennessCentrality, HugeAtomicDoubleArray, CONFIG> computeResult
    ) {
        if (computeResult.result() != null) {
            procResultBuilder.withCentralityFunction(computeResult.result()::get);
        }
        return procResultBuilder;
    }

    static <CONFIG extends BetweennessCentralityBaseConfig> ValidationConfiguration<CONFIG> getValidationConfig() {
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

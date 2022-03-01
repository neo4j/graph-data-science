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
package org.neo4j.gds.centrality;

import org.neo4j.gds.GraphAlgorithmFactory;
import org.neo4j.gds.api.NodeProperties;
import org.neo4j.gds.core.utils.paged.HugeDoubleArray;
import org.neo4j.gds.executor.ComputationResult;
import org.neo4j.gds.executor.validation.BeforeLoadValidation;
import org.neo4j.gds.executor.validation.GraphProjectConfigValidations;
import org.neo4j.gds.executor.validation.ValidationConfiguration;
import org.neo4j.gds.impl.closeness.ClosenessCentralityConfig;
import org.neo4j.gds.impl.closeness.ClosenessCentralityFactory;
import org.neo4j.gds.impl.closeness.ClosenessCentrality;
import org.neo4j.gds.result.AbstractCentralityResultBuilder;

import java.util.List;

public final class ClosenessCentralityProc {

    static final String DESCRIPTION =
        "Closeness centrality is a way of detecting nodes that are " +
        "able to spread information very efficiently through a graph.";

    private ClosenessCentralityProc() {}

    static <CONFIG extends ClosenessCentralityConfig> NodeProperties nodeProperties(ComputationResult<ClosenessCentrality, ClosenessCentrality, CONFIG> computeResult) {
        return computeResult.result().getCentrality().asNodeProperties();
    }

    static <CONFIG extends ClosenessCentralityConfig> GraphAlgorithmFactory<ClosenessCentrality, CONFIG> algorithmFactory() {
        return new ClosenessCentralityFactory<>();
    }

    static <PROC_RESULT, CONFIG extends ClosenessCentralityConfig> AbstractCentralityResultBuilder<PROC_RESULT> resultBuilder(
        AbstractCentralityResultBuilder<PROC_RESULT> procResultBuilder,
        ComputationResult<ClosenessCentrality, ClosenessCentrality, CONFIG> computeResult
    ) {
        if (computeResult.result() != null) {
            HugeDoubleArray centrality = computeResult.result().getCentrality();
            procResultBuilder.withCentralityFunction(centrality::get);
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

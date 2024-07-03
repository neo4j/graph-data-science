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
package org.neo4j.gds.procedures.algorithms.runners;

import org.neo4j.gds.api.AlgorithmMetaDataSetter;
import org.neo4j.gds.applications.algorithms.machinery.ResultBuilder;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.procedures.algorithms.AlgorithmHandle;

import java.util.Map;
import java.util.function.Function;

public class MetadataSetter implements AlgorithmExecutionScaffolding {
    public final AlgorithmMetaDataSetter algorithmMetaDataSetter;
    public final AlgorithmExecutionScaffolding delegate;

    public MetadataSetter(AlgorithmMetaDataSetter algorithmMetaDataSetter, AlgorithmExecutionScaffolding delegate) {
        this.algorithmMetaDataSetter = algorithmMetaDataSetter;
        this.delegate = delegate;
    }

    /**
     * We need to shoehorn in a call to the algorithm metadata setter, which is something we do for stream mode
     */
    @Override
    public <CONFIGURATION extends AlgoBaseConfig, RESULT_FROM_ALGORITHM, RESULT_TO_CALLER, MUTATE_OR_WRITE_METADATA> RESULT_TO_CALLER runAlgorithm(
        String graphNameAsString,
        Map<String, Object> rawConfiguration,
        Function<CypherMapWrapper, CONFIGURATION> configurationSupplier,
        AlgorithmHandle<CONFIGURATION, RESULT_FROM_ALGORITHM, RESULT_TO_CALLER, MUTATE_OR_WRITE_METADATA> algorithm,
        ResultBuilder<CONFIGURATION, RESULT_FROM_ALGORITHM, RESULT_TO_CALLER, MUTATE_OR_WRITE_METADATA> resultBuilder
    ) {
        return delegate.runAlgorithm(
            graphNameAsString,
            rawConfiguration,
            configurationSupplier,
            (graphName, configuration, __) -> {
                // the shoehorning
                algorithmMetaDataSetter.set(configuration);

                return algorithm.compute(graphName, configuration, resultBuilder);
            },
            resultBuilder
        );
    }
}

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
package org.neo4j.gds.procedures.algorithms.stubs;

import org.neo4j.gds.api.GraphName;
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmEstimationTemplate;
import org.neo4j.gds.applications.algorithms.machinery.MemoryEstimateResult;
import org.neo4j.gds.applications.algorithms.machinery.ResultBuilder;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.mem.MemoryEstimation;
import org.neo4j.gds.procedures.algorithms.AlgorithmHandle;
import org.neo4j.gds.procedures.algorithms.configuration.UserSpecificConfigurationParser;

import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;

public final class GenericStub {
    private final UserSpecificConfigurationParser configurationParser;
    private final AlgorithmEstimationTemplate algorithmEstimationTemplate;

    public GenericStub(
        UserSpecificConfigurationParser configurationParser,
        AlgorithmEstimationTemplate algorithmEstimationTemplate
    ) {
        this.configurationParser = configurationParser;
        this.algorithmEstimationTemplate = algorithmEstimationTemplate;
    }

    /**
     * @see org.neo4j.gds.procedures.algorithms.stubs.MutateStub#parseConfiguration(java.util.Map)
     */
    public <CONFIGURATION extends AlgoBaseConfig> CONFIGURATION parseConfiguration(
        Function<CypherMapWrapper, CONFIGURATION> configurationLexer,
        Map<String, Object> configuration
    ) {
        return configurationParser.parseConfiguration(
            configuration,
            configurationLexer
        );
    }

    /**
     * @see org.neo4j.gds.procedures.algorithms.stubs.MutateStub#getMemoryEstimation(String, java.util.Map)
     */
    public <CONFIGURATION extends AlgoBaseConfig> MemoryEstimation getMemoryEstimation(
        Map<String, Object> rawConfiguration,
        Function<CypherMapWrapper, CONFIGURATION> configurationLexer,
        Function<CONFIGURATION, MemoryEstimation> estimator
    ) {
        var configuration = configurationParser.parseConfigurationWithoutDefaultsAndLimits(
            rawConfiguration,
            configurationLexer
        );

        return estimator.apply(configuration);
    }

    /**
     * @see org.neo4j.gds.procedures.algorithms.stubs.MutateStub#estimate(Object, java.util.Map)
     */
    public <CONFIGURATION extends AlgoBaseConfig> Stream<MemoryEstimateResult> estimate(
        Object graphName,
        Map<String, Object> rawConfiguration,
        Function<CypherMapWrapper, CONFIGURATION> configurationLexer,
        Function<CONFIGURATION, MemoryEstimation> estimator
    ) {
        var memoryEstimation = getMemoryEstimation(rawConfiguration, configurationLexer, estimator);

        var configuration = parseConfiguration(configurationLexer, rawConfiguration);

        var memoryEstimateResult = algorithmEstimationTemplate.estimate(
            configuration,
            graphName,
            memoryEstimation
        );

        return Stream.of(memoryEstimateResult);
    }

    /**
     * NB: no configuration validation hook
     */
    public <CONFIGURATION extends AlgoBaseConfig, RESULT_FROM_ALGORITHM, RESULT_TO_CALLER, MUTATE_OR_WRITE_METADATA> Stream<RESULT_TO_CALLER> execute(
        String graphNameAsString,
        Map<String, Object> rawConfiguration,
        Function<CypherMapWrapper, CONFIGURATION> configurationLexer,
        AlgorithmHandle<CONFIGURATION, RESULT_FROM_ALGORITHM, RESULT_TO_CALLER, MUTATE_OR_WRITE_METADATA> executor,
        ResultBuilder<CONFIGURATION, RESULT_FROM_ALGORITHM, RESULT_TO_CALLER, MUTATE_OR_WRITE_METADATA> resultBuilder
    ) {
        var graphName = GraphName.parse(graphNameAsString);
        var configuration = configurationParser.parseConfiguration(
            rawConfiguration,
            configurationLexer
        );

        var result = executor.compute(graphName, configuration, resultBuilder);

        return Stream.of(result);
    }


}

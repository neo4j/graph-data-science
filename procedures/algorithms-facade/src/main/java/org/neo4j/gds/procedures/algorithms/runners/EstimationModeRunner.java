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

import org.neo4j.gds.applications.algorithms.machinery.MemoryEstimateResult;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.procedures.algorithms.configuration.ConfigurationCreator;

import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

public class EstimationModeRunner {
    private final ConfigurationCreator configurationCreator;

    public EstimationModeRunner(ConfigurationCreator configurationCreator) {
        this.configurationCreator = configurationCreator;
    }

    public <CONFIGURATION extends AlgoBaseConfig> MemoryEstimateResult runEstimation(
        Map<String, Object> algorithmConfiguration,
        Function<CypherMapWrapper, CONFIGURATION> configurationParser,
        Function<CONFIGURATION, MemoryEstimateResult> supplier
    ) {
        var configuration = configurationCreator.createConfiguration(
            algorithmConfiguration,
            configurationParser,
            Optional.empty()
        );

        return supplier.apply(configuration);
    }
}

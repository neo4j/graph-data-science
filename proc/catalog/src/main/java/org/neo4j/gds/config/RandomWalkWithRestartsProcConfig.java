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
package org.neo4j.gds.config;

import org.immutables.value.Value;
import org.neo4j.gds.annotation.Configuration;
import org.neo4j.gds.api.GraphStoreFactory;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.graphsampling.config.RandomWalkWithRestartsConfig;

@Configuration
public interface RandomWalkWithRestartsProcConfig extends GraphProjectConfig, GraphNameConfig {
    @Configuration.Parameter
    RandomWalkWithRestartsConfig randomWalkConfig();

    @Configuration.Parameter
    GraphProjectConfig originalConfig();

    @Configuration.Parameter
    String fromGraphName();

    @Value.Default
    @Configuration.Ignore
    @Override
    default GraphStoreFactory.Supplier graphStoreFactory() {
        return originalConfig().graphStoreFactory();
    }

    //TODO: dont understand this. do we need to add another case to Cases for sampled graph ?
    @Override
    @Configuration.Ignore
    default <R> R accept(Cases<R> visitor) {
        return originalConfig().accept(visitor);
    }

    static RandomWalkWithRestartsProcConfig of(
        String userName,
        String graphName,
        String fromGraphName,
        GraphProjectConfig originalConfig,
        CypherMapWrapper procedureConfig
    ) {
        var rwrConfig = RandomWalkWithRestartsConfig.of(procedureConfig);
        return new RandomWalkWithRestartsProcConfigImpl(
            rwrConfig,
            originalConfig,
            fromGraphName,
            userName,
            graphName,
            procedureConfig
        );
    }

}

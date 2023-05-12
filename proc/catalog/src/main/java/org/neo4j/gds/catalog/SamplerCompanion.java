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
package org.neo4j.gds.catalog;

import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.graphsampling.RandomWalkBasedNodesSampler;
import org.neo4j.gds.graphsampling.config.CommonNeighbourAwareRandomWalkConfig;
import org.neo4j.gds.graphsampling.config.RandomWalkWithRestartsConfig;
import org.neo4j.gds.graphsampling.samplers.rw.cnarw.CommonNeighbourAwareRandomWalk;
import org.neo4j.gds.graphsampling.samplers.rw.rwr.RandomWalkWithRestarts;

import java.util.function.Function;

public class SamplerCompanion {

    public static final Function<CypherMapWrapper, RandomWalkWithRestartsConfig> RWR_CONFIG_PROVIDER =
        (cypherMapWrapper) -> RandomWalkWithRestartsConfig.of(cypherMapWrapper);
    public static final Function<CypherMapWrapper, RandomWalkWithRestartsConfig> CNARW_CONFIG_PROVIDER =
        (cypherMapWrapper) -> CommonNeighbourAwareRandomWalkConfig.of(cypherMapWrapper);

    public static final Function<RandomWalkWithRestartsConfig, RandomWalkBasedNodesSampler> RWR_PROVIDER =
        (rwrConfig) -> new RandomWalkWithRestarts(rwrConfig);
    public static final Function<RandomWalkWithRestartsConfig, RandomWalkBasedNodesSampler> CNARW_PROVIDER =
        (cnarwConfig) -> new CommonNeighbourAwareRandomWalk((CommonNeighbourAwareRandomWalkConfig) cnarwConfig);

}

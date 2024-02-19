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
package org.neo4j.gds.applications.graphstorecatalog;

import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.graphsampling.RandomWalkBasedNodesSampler;
import org.neo4j.gds.graphsampling.RandomWalkSamplerType;
import org.neo4j.gds.graphsampling.config.CommonNeighbourAwareRandomWalkConfig;
import org.neo4j.gds.graphsampling.config.RandomWalkWithRestartsConfig;
import org.neo4j.gds.graphsampling.samplers.rw.cnarw.CommonNeighbourAwareRandomWalk;
import org.neo4j.gds.graphsampling.samplers.rw.rwr.RandomWalkWithRestarts;

import static org.neo4j.gds.graphsampling.RandomWalkSamplerType.CNARW;
import static org.neo4j.gds.graphsampling.RandomWalkSamplerType.RWR;

public interface SamplerProvider {

    RandomWalkWithRestartsConfig config();

    RandomWalkBasedNodesSampler algorithm();

    static SamplerProvider of(RandomWalkSamplerType samplerType, CypherMapWrapper cypherMapWrapper) {

        if (samplerType == CNARW) {
            return new CNARWProvider(cypherMapWrapper);
        } else if (samplerType == RWR) {
            return new RWRProvider(cypherMapWrapper);
        }

        throw new RuntimeException();
    }


}

class CNARWProvider implements SamplerProvider {
    private final CommonNeighbourAwareRandomWalkConfig config;

    CNARWProvider(CypherMapWrapper cypherMapWrapper) {
        this.config = CommonNeighbourAwareRandomWalkConfig.of(cypherMapWrapper);
    }

    @Override
    public RandomWalkWithRestartsConfig config() {
        return config;
    }

    @Override
    public RandomWalkBasedNodesSampler algorithm() {
        return new CommonNeighbourAwareRandomWalk(config);
    }
}

class RWRProvider implements SamplerProvider {
    private final RandomWalkWithRestartsConfig config;

    public RWRProvider(CypherMapWrapper cypherMapWrapper) {
        this.config = RandomWalkWithRestartsConfig.of(cypherMapWrapper);
    }

    @Override
    public RandomWalkWithRestartsConfig config() {
        return config;
    }

    @Override
    public RandomWalkBasedNodesSampler algorithm() {
        return new RandomWalkWithRestarts(config);
    }
}


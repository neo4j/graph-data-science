/*
 * Copyright (c) 2017-2019 "Neo4j,"
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
package org.neo4j.graphalgo.louvain;

import org.neo4j.graphalgo.BaseAlgoProc;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.impl.louvain.Louvain;
import org.neo4j.graphalgo.impl.louvain.LouvainFactoryNew;
import org.neo4j.graphalgo.newapi.GraphCreateConfig;
import org.neo4j.graphalgo.newapi.LouvainConfigBase;

import java.util.Optional;

abstract class LouvainProcBase<CONFIG extends LouvainConfigBase> extends BaseAlgoProc<Louvain, Louvain, CONFIG> {

    abstract CONFIG newConfig(
        String username,
        Optional<String> graphName,
        Optional<GraphCreateConfig> maybeImplicitCreate,
        CypherMapWrapper config
    );

    @Override
    protected final CONFIG newConfig(Optional<String> graphName, CypherMapWrapper config) {
        Optional<GraphCreateConfig> maybeImplicitCreate = Optional.empty();
        if (!graphName.isPresent()) {
            // we should do implicit loading
            maybeImplicitCreate = Optional.of(GraphCreateConfig.implicitCreate(getUsername(), config));
        }
        return newConfig(getUsername(), graphName, maybeImplicitCreate, config);
    }

    @Override
    protected final LouvainFactoryNew<CONFIG> algorithmFactory(LouvainConfigBase config) {
        Louvain.Config louvainConfig = new Louvain.Config(
            config.maxLevels(),
            config.maxIterations(),
            config.tolerance(),
            config.includeIntermediateCommunities(),
            Optional.ofNullable(config.seedProperty())
        );

        return new LouvainFactoryNew<>(louvainConfig);
    }
}

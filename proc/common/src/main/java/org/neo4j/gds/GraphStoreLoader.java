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
package org.neo4j.gds;

import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.config.GraphCreateConfig;
import org.neo4j.gds.core.GraphDimensions;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;

import java.util.Optional;

public interface GraphStoreLoader {
    GraphCreateConfig graphCreateConfig();
    GraphStore graphStore();
    GraphDimensions graphDimensions();
    Optional<MemoryEstimation> memoryEstimation();

    static GraphStoreLoader of(
        AlgoBaseConfig config,
        Optional<String> maybeGraphName,
        BaseProc baseProc
    ) {
        var username = baseProc.username();
        if (maybeGraphName.isPresent()) {
            return new GraphStoreFromCatalogLoader(
                maybeGraphName.get(),
                config,
                username,
                baseProc.databaseId(),
                baseProc.isGdsAdmin()
            );
        } else if (config.implicitCreateConfig().isPresent()) {
            return implicitGraphStoreLoader(config.implicitCreateConfig().get(), baseProc);
        } else {
            throw new IllegalStateException("There must be either a graph name or an implicit create config");
        }
    }

    static GraphStoreLoader implicitGraphStoreLoader(
        GraphCreateConfig graphCreateConfig,
        BaseProc baseProc
    ) {
        if (graphCreateConfig.isFictitiousLoading()) {
            return new FictitiousGraphStoreLoader(graphCreateConfig);
        } else {
            return new ImplicitGraphStoreLoader(
                graphCreateConfig,
                baseProc.username(),
                baseProc.graphLoaderContext()
            );
        }
    }
}

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
package org.neo4j.gds.api;

import org.neo4j.gds.config.GraphProjectConfig;
import org.neo4j.gds.core.GraphDimensions;
import org.neo4j.gds.core.loading.CSRGraphStore;
import org.neo4j.gds.core.loading.IdMapAndProperties;
import org.neo4j.gds.core.loading.RelationshipsAndProperties;
import org.neo4j.gds.mem.MemoryUsage;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public abstract class CSRGraphStoreFactory<CONFIG extends GraphProjectConfig> extends GraphStoreFactory<CSRGraphStore, CONFIG> {

    public CSRGraphStoreFactory(
        CONFIG graphProjectConfig,
        GraphLoaderContext loadingContext,
        GraphDimensions dimensions
    ) {
        super(graphProjectConfig, loadingContext, dimensions);
    }

    protected CSRGraphStore createGraphStore(
        IdMapAndProperties idMapAndProperties,
        RelationshipsAndProperties relationshipsAndProperties
    ) {
        return CSRGraphStore.of(
            loadingContext.api().databaseId(),
            idMapAndProperties.idMap(),
            idMapAndProperties.properties(),
            relationshipsAndProperties.relationships(),
            relationshipsAndProperties.properties(),
            graphProjectConfig.readConcurrency()
        );
    }

    protected void logLoadingSummary(GraphStore graphStore) {
        var sizeInBytes = MemoryUsage.sizeOf(graphStore);
        var memoryUsage = MemoryUsage.humanReadable(sizeInBytes);
        progressTracker.logMessage(formatWithLocale("Actual memory usage of the loaded graph: %s", memoryUsage));
    }
}

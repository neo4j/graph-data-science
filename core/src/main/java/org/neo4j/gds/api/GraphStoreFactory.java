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

import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.config.GraphProjectConfig;
import org.neo4j.gds.core.GraphDimensions;
import org.neo4j.gds.core.loading.Capabilities;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;

/**
 * The Abstract Factory defines the construction of the graph
 */
public abstract class GraphStoreFactory<STORE extends GraphStore, CONFIG extends GraphProjectConfig> {

    public interface Supplier {
        GraphStoreFactory<? extends GraphStore, ? extends GraphProjectConfig> get(GraphLoaderContext loaderContext);

        default GraphStoreFactory<? extends GraphStore, ? extends GraphProjectConfig> getWithDimension(
            GraphLoaderContext loaderContext,
            GraphDimensions graphDimensions
        ) {
            return get(loaderContext);
        }
    }

    protected final CONFIG graphProjectConfig;
    protected final Capabilities capabilities;
    protected final GraphLoaderContext loadingContext;
    protected final GraphDimensions dimensions;

    public GraphStoreFactory(
        CONFIG graphProjectConfig,
        Capabilities capabilities,
        GraphLoaderContext loadingContext,
        GraphDimensions dimensions
    ) {
        this.graphProjectConfig = graphProjectConfig;
        this.capabilities = capabilities;
        this.loadingContext = loadingContext;
        this.dimensions = dimensions;
    }

    public abstract STORE build();

    public abstract MemoryEstimation estimateMemoryUsageDuringLoading();

    public abstract MemoryEstimation estimateMemoryUsageAfterLoading();

    public GraphDimensions dimensions() {
        return this.dimensions;
    }

    public GraphDimensions estimationDimensions() {
        return dimensions;
    }

    public CONFIG graphProjectConfig() {
        return graphProjectConfig;
    }

    @ValueClass
    public interface ImportResult<STORE extends GraphStore> {
        STORE graphStore();

        static <STORE extends GraphStore> ImportResult<STORE> of(STORE graphStore) {
            return ImmutableImportResult.<STORE>builder().graphStore(graphStore).build();
        }
    }
}

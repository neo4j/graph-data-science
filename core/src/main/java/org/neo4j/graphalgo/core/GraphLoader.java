/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
package org.neo4j.graphalgo.core;

import org.immutables.value.Value;
import org.neo4j.graphalgo.annotation.ValueClass;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.GraphFactory;
import org.neo4j.graphalgo.api.GraphSetup;
import org.neo4j.graphalgo.core.loading.CypherGraphFactory;
import org.neo4j.graphalgo.core.loading.GraphStore;
import org.neo4j.graphalgo.core.loading.HugeGraphFactory;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.config.GraphCreateConfig;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import static org.neo4j.graphalgo.compat.ExceptionUtil.throwIfUnchecked;

@ValueClass
public interface GraphLoader {

    GraphDatabaseAPI api();

    @Value.Default
    default ExecutorService executorService() {
        return Pools.DEFAULT;
    }

    @Value.Default
    default AllocationTracker tracker() {
        return AllocationTracker.EMPTY;
    }

    @Value.Default
    default TerminationFlag terminationFlag() {
        return TerminationFlag.RUNNING_TRUE;
    }

    @Value.Default
    default Map<String, Object> params() {
        return new HashMap<>();
    }

    @Value.Default
    default String username() {
        return createConfig().username();
    }

    Log log();

    GraphCreateConfig createConfig();

    KernelTransaction kernelTransaction();

    default Graph graph(Class<? extends GraphFactory> factoryType) {
        return load(factoryType);
    }

    default GraphStore graphStore(Class<? extends GraphFactory> factoryType) {
        return build(factoryType).build().graphStore();
    }

    @Value.Lazy
    default GraphSetup toSetup() {
        return new GraphSetup(
            params(),
            executorService(),
            log(),
            tracker(),
            terminationFlag(),
            createConfig()
        );
    }

    /**
     * Returns an instance of the factory that can be used to load the graph.
     */
    default <T extends GraphFactory> T build(final Class<T> factoryType) {
        try {
            GraphSetup setup = toSetup();
            GraphFactory factory;

            if (CypherGraphFactory.class.isAssignableFrom(factoryType)) {
                factory = new CypherGraphFactory(api(), setup, kernelTransaction());
            } else {
                factory = new HugeGraphFactory(api(), setup);
            }

            return factoryType.cast(factory);
        } catch (Throwable throwable) {
            throwIfUnchecked(throwable);
            throw new RuntimeException(throwable.getMessage(), throwable);
        }
    }

    /**
     * Loads the graph using the provided GraphFactory, passing the built
     * configuration as parameters.
     * <p>
     * The chosen implementation determines the performance characteristics
     * during load and usage of the Graph.
     *
     * @return the freshly loaded graph
     */
    default Graph load(Class<? extends GraphFactory> factoryType) {
        return build(factoryType).build().graphStore().getUnion();
    }

}

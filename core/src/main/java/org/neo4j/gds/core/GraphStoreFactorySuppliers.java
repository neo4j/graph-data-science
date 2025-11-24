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
package org.neo4j.gds.core;

import org.neo4j.gds.api.GraphStoreFactory;
import org.neo4j.gds.config.GraphProjectConfig;

import java.util.Map;
import java.util.function.Function;

/**
 * This is where you load up your GraphStoreFactorySuppliers. And you access them using a key.
 */
public class GraphStoreFactorySuppliers {
    private final Map<String, Function<GraphProjectConfig, GraphStoreFactory.Supplier>> suppliers;

    public GraphStoreFactorySuppliers(Map<String, Function<GraphProjectConfig, GraphStoreFactory.Supplier>> suppliers) {
        this.suppliers = suppliers;
    }

    /**
     * @param key we switch based on type
     * @return the stub needed to obtain a {@link org.neo4j.gds.api.GraphStoreFactory}
     * @throws java.lang.IllegalArgumentException if no supplier matches the given key
     */
    public GraphStoreFactory.Supplier find(GraphProjectConfig key) {
        var keyAsString = key.getClass().getSimpleName();

        // not great perhaps, but the other one uses instanceof :shrug:
        if (suppliers.containsKey(keyAsString)) return suppliers.get(keyAsString).apply(key);

        // programmer error if we get here
        throw new IllegalStateException("unable to supply graph store factory for " + keyAsString);
    }
}

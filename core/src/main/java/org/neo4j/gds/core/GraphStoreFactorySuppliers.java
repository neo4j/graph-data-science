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

/**
 * This is where you load up your GraphStoreFactorySuppliers. And you access them using a key.
 */
public class GraphStoreFactorySuppliers {
    public GraphStoreFactorySuppliers() {

    }

    /**
     * @param key we switch based on type
     * @return the stub needed to obtain a {@link org.neo4j.gds.api.GraphStoreFactory}
     * @throws java.lang.IllegalArgumentException if no supplier matches the given key
     */
    public GraphStoreFactory.Supplier find(GraphProjectConfig key) {
        try {
            // in the short term we just reuse the existing stuff,
            // we can change it once dependency injection is in place
            return GraphStoreFactorySupplier.supplier(key);
        } catch (UnsupportedOperationException e) {
            throw new IllegalArgumentException(e.getMessage());
        }
    }
}

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
package org.neo4j.graphalgo.core;

import org.immutables.value.Value;
import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.GraphLoaderContext;
import org.neo4j.graphalgo.api.GraphStore;
import org.neo4j.graphalgo.api.GraphStoreFactory;
import org.neo4j.graphalgo.config.GraphCreateConfig;

@ValueClass
public interface GraphLoader {

    GraphLoaderContext context();

    @Value.Default
    default String username() {
        return createConfig().username();
    }

    GraphCreateConfig createConfig();

    default Graph graph() {
        return graphStore().getUnion();
    }

    default GraphStore graphStore() {
        return graphStoreFactory().build().graphStore();
    }

    default GraphStoreFactory<? extends GraphStore, ? extends GraphCreateConfig> graphStoreFactory() {
        return createConfig().graphStoreFactory().get(context());
    }

}

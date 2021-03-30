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
package org.neo4j.graphalgo.gdl;

import org.immutables.value.Value;
import org.neo4j.graphalgo.Orientation;
import org.neo4j.graphalgo.annotation.Configuration;
import org.neo4j.graphalgo.annotation.ValueClass;
import org.neo4j.graphalgo.api.GraphStoreFactory;
import org.neo4j.graphalgo.config.GraphCreateConfig;
import org.neo4j.graphalgo.core.Aggregation;

@ValueClass
@SuppressWarnings("immutables:subtype")
public interface GraphCreateFromGdlConfig extends GraphCreateConfig {

    String gdlGraph();

    @Value.Default
    default Orientation orientation() {
        return Orientation.NATURAL;
    }

    @Value.Default
    default Aggregation aggregation() {
        return Aggregation.NONE;
    }

    @Override
    default GraphStoreFactory.Supplier graphStoreFactory() {
        return loaderContext -> GdlFactory
            .builder()
            .createConfig(this)
            .namedDatabaseId(loaderContext.api().databaseId())
            .build();
    }

    @Override
    @Configuration.Ignore
    default <R> R accept(GraphCreateConfig.Cases<R> cases) {
        if (cases instanceof Cases) {
            return ((Cases<R>) cases).gdl(this);
        }
        throw new IllegalArgumentException("Expected Visitor of type " + Cases.class.getName());
    }

    interface Cases<R> extends GraphCreateConfig.Cases<R> {

        R gdl(GraphCreateFromGdlConfig gdlConfig);
    }
}

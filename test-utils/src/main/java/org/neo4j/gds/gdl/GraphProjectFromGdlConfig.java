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
package org.neo4j.gds.gdl;

import org.immutables.value.Value;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.annotation.Configuration;
import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.api.GraphStoreFactory;
import org.neo4j.gds.config.GraphProjectConfig;
import org.neo4j.gds.core.Aggregation;

@ValueClass
@SuppressWarnings("immutables:subtype")
public interface GraphProjectFromGdlConfig extends GraphProjectConfig {

    String gdlGraph();

    @Value.Default
    default Orientation orientation() {
        return Orientation.NATURAL;
    }

    @Value.Default
    default Aggregation aggregation() {
        return Aggregation.DEFAULT;
    }

    @Override
    default GraphStoreFactory.Supplier graphStoreFactory() {
        return loaderContext -> GdlFactory
            .builder()
            .graphProjectConfig(this)
            .databaseId(DatabaseId.of(loaderContext.graphDatabaseService()))
            .build();
    }

    @Override
    @Configuration.Ignore
    default <R> R accept(GraphProjectConfig.Cases<R> cases) {
        if (cases instanceof Cases) {
            return ((Cases<R>) cases).gdl(this);
        }
        throw new IllegalArgumentException("Expected Visitor of type " + Cases.class.getName());
    }

    interface Cases<R> extends GraphProjectConfig.Cases<R> {

        R gdl(GraphProjectFromGdlConfig gdlConfig);
    }
}

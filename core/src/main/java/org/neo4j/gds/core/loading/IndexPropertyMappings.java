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
package org.neo4j.gds.core.loading;

import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.PropertyMapping;
import org.neo4j.gds.PropertyMappings;
import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.config.GraphProjectFromStoreConfig;
import org.neo4j.internal.schema.IndexDescriptor;

import java.util.List;
import java.util.Map;

import static java.util.stream.Collectors.toMap;

// TODO: should be named LoadablePropertyMappings
// TODO: remove
final class IndexPropertyMappings {

    static Map<NodeLabel, PropertyMappings> propertyMappings(GraphProjectFromStoreConfig graphProjectConfig) {
        return graphProjectConfig
            .nodeProjections()
            .projections()
            .entrySet()
            .stream()
            .collect(toMap(
                Map.Entry::getKey,
                entry -> entry
                    .getValue()
                    .properties()
            ));
    }

    static LoadablePropertyMappings prepareProperties(GraphProjectFromStoreConfig graphProjectConfig) {
        var storeLoadedProperties = propertyMappings(graphProjectConfig);
        
        return ImmutableLoadablePropertyMappings
            .builder()
            .putAllStoredProperties(storeLoadedProperties)
            .build();
    }

    private IndexPropertyMappings() {}

    @ValueClass
    interface IndexedPropertyMapping {

        PropertyMapping property();

        IndexDescriptor index();
    }

    @ValueClass
    interface IndexedPropertyMappings {

        List<IndexedPropertyMapping> mappings();
    }

    @ValueClass
    public interface LoadablePropertyMappings {

        Map<NodeLabel, PropertyMappings> storedProperties();

        // TODO: remove
        Map<NodeLabel, IndexedPropertyMappings> indexedProperties();
    }
}



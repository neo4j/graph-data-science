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
package org.neo4j.gds.core.io;

import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.schema.NodeSchema;
import org.neo4j.gds.api.schema.PropertySchema;
import org.neo4j.gds.api.schema.RelationshipSchema;
import org.neo4j.gds.core.io.file.GraphInfo;
import org.neo4j.gds.core.io.file.ImmutableGraphInfo;

import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

@ValueClass
public interface MetaDataStore {
    GraphInfo graphInfo();
    NodeSchema nodeSchema();
    RelationshipSchema relationshipSchema();
    Map<String, PropertySchema> graphPropertySchema();

    static MetaDataStore of(GraphStore graphStore) {
        var relTypeCounts = graphStore.relationshipTypes().stream().collect(Collectors.toMap(
            Function.identity(),
            graphStore::relationshipCount
        ));
        GraphInfo graphInfo = ImmutableGraphInfo.of(
            graphStore.databaseId(),
            graphStore.nodeCount(),
            graphStore.nodes().highestOriginalId(),
            relTypeCounts
        );
        var schema = graphStore.schema();
        return ImmutableMetaDataStore.of(
            graphInfo,
            schema.nodeSchema(),
            schema.relationshipSchema(),
            schema.graphProperties()
        );
    }
}

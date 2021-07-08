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
package org.neo4j.graphalgo.core.utils.io;

import org.neo4j.graphalgo.annotation.ValueClass;
import org.neo4j.graphalgo.api.GraphStore;
import org.neo4j.graphalgo.api.schema.NodeSchema;
import org.neo4j.graphalgo.api.schema.RelationshipSchema;
import org.neo4j.graphalgo.core.loading.IdMapImplementations;
import org.neo4j.graphalgo.core.utils.io.file.GraphInfo;
import org.neo4j.graphalgo.core.utils.io.file.ImmutableGraphInfo;

@ValueClass
public interface MetaDataStore {
    GraphInfo graphInfo();
    NodeSchema nodeSchema();
    RelationshipSchema relationshipSchema();

    static MetaDataStore of(GraphStore graphStore) {
        GraphInfo graphInfo = ImmutableGraphInfo.of(
            graphStore.databaseId(),
            graphStore.nodeCount(),
            graphStore.nodes().highestNeoId(),
            IdMapImplementations.useBitIdMap()
        );
        NodeSchema nodeSchema = graphStore.schema().nodeSchema();
        RelationshipSchema relationshipSchema = graphStore.schema().relationshipSchema();
        return ImmutableMetaDataStore.of(graphInfo, nodeSchema, relationshipSchema);
    }
}

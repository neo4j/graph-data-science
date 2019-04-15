/*
 * Copyright (c) 2017 "Neo4j, Inc." <http://neo4j.com>
 *
 * This file is part of Neo4j Graph Algorithms <http://github.com/neo4j-contrib/neo4j-graph-algorithms>.
 *
 * Neo4j Graph Algorithms is free software: you can redistribute it and/or modify
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
package org.neo4j.kernel.impl.newapi;

import org.neo4j.kernel.impl.store.id.IdGenerator;
import org.neo4j.kernel.impl.store.id.IdGeneratorFactory;
import org.neo4j.kernel.impl.store.id.IdType;
import org.neo4j.kernel.impl.util.UnsatisfiedDependencyException;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.OptionalLong;

public final class InternalReadOps {

    public static long getHighestPossibleNodeCount(org.neo4j.internal.kernel.api.Read read, GraphDatabaseAPI api) {
        return nodeCountByIdGenerator(read, api);
    }

    public static long getHighestPossibleRelationshipCount(org.neo4j.internal.kernel.api.Read read, GraphDatabaseAPI api) {
        return relationshipCountByIdGenerator(read, api);
    }

    private static long nodeCountByIdGenerator(org.neo4j.internal.kernel.api.Read read, GraphDatabaseAPI api) {
        return countByIdGenerator(api, IdType.NODE).orElseGet(read::nodesGetCount);
    }

    private static long relationshipCountByIdGenerator(org.neo4j.internal.kernel.api.Read read, GraphDatabaseAPI api) {
        return countByIdGenerator(api, IdType.RELATIONSHIP).orElseGet(read::relationshipsGetCount);
    }

    private static OptionalLong countByIdGenerator(GraphDatabaseAPI api, IdType idType) {
        if (api != null) {
            try {
                IdGeneratorFactory idGeneratorFactory = api
                        .getDependencyResolver()
                        .resolveDependency(IdGeneratorFactory.class);
                if (idGeneratorFactory != null) {
                    final IdGenerator idGenerator = idGeneratorFactory.get(idType);
                    if (idGenerator != null) {
                        return OptionalLong.of(idGenerator.getHighId());
                    }
                }
            } catch (IllegalArgumentException | UnsatisfiedDependencyException ignored) {
            }
        }
        return OptionalLong.empty();
    }

    private InternalReadOps() {
        throw new UnsupportedOperationException("No instances");
    }
}

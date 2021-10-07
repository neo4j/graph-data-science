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
package org.neo4j.gds.compat;

import org.jetbrains.annotations.Nullable;
import org.neo4j.internal.id.IdGenerator;
import org.neo4j.internal.id.IdGeneratorFactory;
import org.neo4j.internal.id.IdType;

import java.util.OptionalLong;

public final class InternalReadOps {

    public static OptionalLong countByIdGenerator(@Nullable IdGeneratorFactory idGeneratorFactory, IdType idType) {
        if (idGeneratorFactory != null) {
            try {
                final IdGenerator idGenerator = idGeneratorFactory.get(idType);
                if (idGenerator != null) {
                    // getHighId returns the highestId + 1, which is actually a count
                    return OptionalLong.of(idGenerator.getHighId());
                }
            } catch (Exception ignored) {
            }
        }
        return OptionalLong.empty();
    }

    private InternalReadOps() {
        throw new UnsupportedOperationException("No instances");
    }
}

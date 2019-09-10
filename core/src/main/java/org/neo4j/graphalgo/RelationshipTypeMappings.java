/*
 * Copyright (c) 2017-2019 "Neo4j,"
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

package org.neo4j.graphalgo;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

import static org.neo4j.kernel.api.StatementConstants.NO_SUCH_RELATIONSHIP_TYPE;

public final class RelationshipTypeMappings implements Iterable<RelationshipTypeMapping> {

    private static final RelationshipTypeMappings ALL = new RelationshipTypeMappings(RelationshipTypeMapping.all());

    private final RelationshipTypeMapping[] mappings;

    public static RelationshipTypeMappings of(RelationshipTypeMapping... mappings) {
        if (mappings == null || mappings.length == 0) {
            return ALL;
        }
        return new RelationshipTypeMappings(mappings);
    }

    private RelationshipTypeMappings(RelationshipTypeMapping... mappings) {
        this.mappings = mappings;
    }

    public Stream<RelationshipTypeMapping> stream() {
        return Arrays.stream(mappings);
    }

    @Override
    public Iterator<RelationshipTypeMapping> iterator() {
        return stream().iterator();
    }

    public boolean isMultipleTypes() {
        return mappings.length > 1;
    }

    public int[] relationshipTypeIds() {
        // type ids are only used for heavy importer, which will be removed soon-ish.
        // Heavy loader expecteds only a valid single types entry (for loading that type)
        // or an empty array for loading all types.
        // We check for the typeId here instead of doesExist to catch both cases.
        return Arrays.stream(mappings)
                .filter(m -> m.typeId() != NO_SUCH_RELATIONSHIP_TYPE)
                .mapToInt(RelationshipTypeMapping::typeId)
                .limit(1)
                .toArray();
    }

    public static final class Builder {
        private final List<RelationshipTypeMapping> mappings;

        public Builder() {
            mappings = new ArrayList<>();
        }

        public void addMapping(RelationshipTypeMapping mapping) {
            mappings.add(Objects.requireNonNull(mapping, "mapping"));
        }

        public RelationshipTypeMappings build() {
            RelationshipTypeMapping[] mappings = this.mappings.toArray(new RelationshipTypeMapping[0]);
            return of(mappings);
        }
    }
}

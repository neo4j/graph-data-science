/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
package org.neo4j.gds.core.pagecached;

import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.BaseTest;
import org.neo4j.graphalgo.compat.GraphDatabaseApiProxy;
import org.neo4j.io.pagecache.PageCache;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertFalse;

class ReverseIdMappingBuilderTest extends BaseTest {

    @Test
    void initializesInterleavedPages() throws IOException {
        PageCache pageCache = GraphDatabaseApiProxy.resolveDependency(db, PageCache.class);

        ReverseIdMapping.Builder builder = ReverseIdMapping.Builder.create(pageCache, 3072);
        builder.set(0, 0);    // Sets and id on the first page and initializes it
        builder.set(2049, 1); // Sets and id on the third page and initializes it

        ReverseIdMapping reverseIdMapping = builder.build();
        assertFalse(reverseIdMapping.contains(2044)); // Tries to read from the second page which isn't initialized by a set operation
    }

}
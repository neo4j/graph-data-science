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
package org.neo4j.gds.assertions;

import org.assertj.core.api.AbstractAssert;
import org.assertj.core.api.Assertions;
import org.neo4j.gds.core.utils.mem.MemoryTree;

import java.util.stream.Collectors;

public class MemoryTreeAssert extends AbstractAssert<MemoryTreeAssert, MemoryTree> {


    private MemoryTreeAssert(MemoryTree memoryTree) {
        super(memoryTree, MemoryTreeAssert.class);
    }

    public static MemoryTreeAssert assertThat(MemoryTree actual) {
        return new MemoryTreeAssert(actual);
    }

    public MemoryRangeAssert memoryRange() {
        isNotNull();
        return  MemoryRangeAssert.assertThat(actual.memoryUsage());
    }

    public  MemoryTreeAssert componentsDescriptionsContainExactly(String... expectedDescriptions){
        isNotNull();
        var componentsDescription = actual.components().stream().map(MemoryTree::description).collect(Collectors.toList());

        Assertions.assertThat(componentsDescription).containsExactly(expectedDescriptions);

        return  this;

    }

}

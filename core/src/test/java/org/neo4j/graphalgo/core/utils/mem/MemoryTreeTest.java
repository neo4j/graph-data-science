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
package org.neo4j.graphalgo.core.utils.mem;

import org.junit.Test;
import org.neo4j.helpers.collection.MapUtil;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.hasItem;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public final class MemoryTreeTest {

    @Test
    public void renderLeaf() {
        MemoryTree memoryTree = new LeafTree("description", MemoryRange.of(12L));
        Map<String, Object> value = memoryTree.renderMap();

        assertEquals("description", value.get("name").toString());
        assertEquals("12 Bytes", value.get("memoryUsage").toString());
    }

    @Test
    public void renderChildren() {
        MemoryTree level2A = new LeafTree("level2A", MemoryRange.of(12L));
        MemoryTree level2B = new LeafTree("level2B", MemoryRange.of(22L));

        CompositeTree compositeTree = new CompositeTree("level1", Arrays.asList(level2A, level2B));

        Map<String, Object> value = compositeTree.renderMap();

        assertEquals("level1", value.get("name").toString());
        assertEquals("34 Bytes", value.get("memoryUsage").toString());

        List<Map<String, Object>> components = (List<Map<String, Object>>) value.get("components");

        assertThat(components, hasItem(MapUtil.map("name", "level2A", "memoryUsage", "12 Bytes")));
        assertThat(components, hasItem(MapUtil.map("name", "level2B", "memoryUsage", "22 Bytes")));
    }
}

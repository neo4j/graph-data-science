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
package org.neo4j.graphalgo.core.utils.export.file;

import org.junit.jupiter.api.Test;
import org.neo4j.internal.batchimport.input.InputEntityVisitor;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class RelationshipLineChunkTest {

    @Test
    void shouldVisitLine() throws IOException {
        var line = "0,1,19.19,42";
        var header = RelationshipFileHeader.builder()
            .withRelationshipType("REL")
            .withHeaderLine(":START_ID,:END_ID,foo:DOUBLE,bar:LONG")
            .build();

        var lineChunk = new FileInput.RelationshipLineChunk();
        var visitor = new TestRelationshipVisitor();
        lineChunk.visitLine(line, header, visitor);

        assertThat(visitor.startId).isEqualTo(0);
        assertThat(visitor.endId).isEqualTo(1);
        assertThat(visitor.properties).containsExactlyInAnyOrderEntriesOf(
            Map.of(
                "foo", "19.19",
                "bar", "42"
            )
        );
    }

    private static class TestRelationshipVisitor extends InputEntityVisitor.Adapter {
        private long startId;
        private long endId;
        private final Map<String, Object> properties = new HashMap<>();

        @Override
        public boolean startId(long id) {
            this.startId = id;
            return super.startId(id);
        }

        @Override
        public boolean endId(long id) {
            this.endId = id;
            return super.endId(id);
        }

        @Override
        public boolean property(String key, Object value) {
            this.properties.put(key, value);
            return super.property(key, value);
        }

    }
}

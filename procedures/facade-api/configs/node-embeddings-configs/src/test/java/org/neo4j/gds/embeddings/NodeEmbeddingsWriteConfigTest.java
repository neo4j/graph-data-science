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
package org.neo4j.gds.embeddings;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.embeddings.fastrp.FastRPWriteConfig;

import java.util.HashMap;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class NodeEmbeddingsWriteConfigTest {

    private static CypherMapWrapper config(boolean writeAsVector, boolean writeToResultStore) {
        var map = new HashMap<String, Object>();
        map.put("writeProperty", "embedding");
        map.put("embeddingDimension", 64);
        map.put("writeAsVector", writeAsVector);
        map.put("writeToResultStore", writeToResultStore);
        return CypherMapWrapper.create(map);
    }

    @Test
    void rejectsWriteAsVectorWhenWritingToResultStore() {
        assertThatThrownBy(() -> FastRPWriteConfig.of(config(true, true)))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("`writeAsVector` is not supported when writing to the result store");
    }

    @Test
    void allowsWriteAsVectorForDirectWrites() {
        assertThatCode(() -> FastRPWriteConfig.of(config(true, false))).doesNotThrowAnyException();
    }

    @Test
    void allowsResultStoreWithoutWriteAsVector() {
        assertThatCode(() -> FastRPWriteConfig.of(config(false, true))).doesNotThrowAnyException();
    }
}

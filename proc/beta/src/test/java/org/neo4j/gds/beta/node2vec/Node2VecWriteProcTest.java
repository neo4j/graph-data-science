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
package org.neo4j.gds.beta.node2vec;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.embeddings.node2vec.Node2Vec;
import org.neo4j.gds.embeddings.node2vec.Node2VecWriteConfig;
import org.neo4j.gds.ml.core.tensor.FloatVector;
import org.neo4j.gds.AlgoBaseProc;
import org.neo4j.graphalgo.GdsCypher;
import org.neo4j.graphalgo.core.utils.paged.HugeObjectArray;
import org.neo4j.graphdb.QueryExecutionException;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;
import static org.neo4j.graphalgo.utils.ExceptionUtil.rootCause;

class Node2VecWriteProcTest extends Node2VecProcTest<Node2VecWriteConfig> {

    @Test
    void embeddingsShouldHaveTheConfiguredDimension() {
        long dimensions = 42;
        var query = GdsCypher.call()
            .loadEverything()
            .algo("gds.beta.node2vec")
            .writeMode()
            .addParameter("writeProperty", "embedding")
            .addParameter("embeddingDimension", dimensions)
            .yields();
        runQuery(query);

        assertCypherResult(
            "MATCH (n) RETURN size(n.embedding) AS size",
            List.of(
                Map.of("size", dimensions),
                Map.of("size", dimensions),
                Map.of("size", dimensions),
                Map.of("size", dimensions),
                Map.of("size", dimensions)
            )
        );
    }

    public Class<? extends AlgoBaseProc<Node2Vec, HugeObjectArray<FloatVector>, Node2VecWriteConfig>> getProcedureClazz() {
        return Node2VecWriteProc.class;
    }

    @Override
    public Node2VecWriteConfig createConfig(CypherMapWrapper userInput) {
        return Node2VecWriteConfig.of(getUsername(), Optional.empty(), Optional.empty(), userInput);
    }

    @Test
    void shouldThrowIfRunningWouldOverflow() {
        long nodeCount = runQuery("MATCH (n) RETURN count(n) AS count", result ->
            result.<Long>columnAs("count").stream().findFirst().orElse(-1L)
        );
        var query = GdsCypher.call()
            .loadEverything()
            .algo("gds.beta.node2vec")
            .writeMode()
            .addParameter("writeProperty", "embedding")
            .addParameter("walksPerNode", Integer.MAX_VALUE)
            .addParameter("walkLength", Integer.MAX_VALUE)
            .addParameter("sudo", true)
            .yields();

        Throwable throwable = rootCause(assertThrows(QueryExecutionException.class, () -> runQuery(query)));
        assertEquals(IllegalArgumentException.class, throwable.getClass());
        String expectedMessage = formatWithLocale(
            "Aborting execution, running with the configured parameters is likely to overflow: node count: %d, walks per node: %d, walkLength: %d." +
            " Try reducing these parameters or run on a smaller graph.",
            nodeCount,
            Integer.MAX_VALUE,
            Integer.MAX_VALUE
        );
        assertEquals(expectedMessage, throwable.getMessage());
    }

    public CypherMapWrapper createMinimalConfig(CypherMapWrapper userInput) {
        if (!userInput.containsKey("writeProperty")) {
            return userInput.withString("writeProperty", "embedding");
        }
        return userInput;
    }
}

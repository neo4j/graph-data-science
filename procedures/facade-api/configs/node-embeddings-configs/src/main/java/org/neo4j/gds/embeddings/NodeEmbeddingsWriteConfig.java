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

import org.neo4j.gds.annotation.Configuration;
import org.neo4j.gds.config.WritePropertyConfig;

/**
 * Shared write configuration for node-embedding write procedures.
 */
public interface NodeEmbeddingsWriteConfig extends WritePropertyConfig {

    /**
     * When {@code true}, the embedding is written as a Neo4j vector property (a {@code Float32Vector}
     * for float embeddings, a {@code Float64Vector} for double embeddings) instead of a plain array,
     * so it can back a vector index. Requires a database whose store format supports vector properties
     * (the block store format).
     */
    default boolean writeAsVector() {
        return false;
    }

    @Configuration.Check
    default void validateWriteAsVector() {
        // Writing into the result store does not carry the vector flag through to the eventual
        // write-back, so reject the misconfiguration up front rather than silently writing arrays.
        if (writeAsVector() && writeToResultStore()) {
            throw new IllegalArgumentException(
                "`writeAsVector` is not supported when writing to the result store."
            );
        }
    }
}

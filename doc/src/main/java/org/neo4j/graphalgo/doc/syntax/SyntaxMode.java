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
package org.neo4j.graphalgo.doc.syntax;

enum SyntaxMode {
    STATS("include-with-stats"),
    STREAM("include-with-stream"),
    MUTATE("include-with-mutate"),
    WRITE("include-with-write"),
    TRAIN("include-with-train"),
    ESTIMATE("estimate-syntax"),
    GRAPH_DROP("graph-drop-syntax"),
    MODEL_DROP("model-drop-syntax"),
    GRAPH_CREATE("graph-create-syntax"),
    GRAPH_CREATE_CYPHER("graph-create-cypher-syntax"),
    STREAM_NODE_PROPERTIES("stream-node-properties-syntax"),
    STREAM_SINGLE_PROPERTY("include-with-stream-single-property"),
    GRAPH_EXISTS("graph-exists-syntax"),
    MODEL_EXISTS("model-exists-syntax"),
    GRAPH_EXPORT("graph-export-syntax"),
    CREATE_SUBGRAPH("create-subgraph-syntax"),
    REMOVE("include-with-remove"),
    DELETE_RELATIONSHIPS("include-with-delete-relationships"),
    GRAPH_LIST("graph-list-syntax"),
    MODEL_LIST("model-list-syntax"),
    MODEL_PUBLISH("model-publish-syntax");

    private final String mode;

    SyntaxMode(String mode) {
        this.mode = mode;
    }

    public String mode() {
        return mode;
    }
}

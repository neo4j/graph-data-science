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
package org.neo4j.gds.doc.syntax;

public enum SyntaxMode {
    STATS("include-with-stats"),
    STREAM("include-with-stream"),
    MUTATE("include-with-mutate"),
    WRITE("include-with-write"),
    TRAIN("include-with-train"),
    ESTIMATE("estimate-syntax"),
    GRAPH_DROP("graph-drop-syntax"),
    MODEL_DROP("model-drop-syntax"),
    GRAPH_PROJECT("graph-project-syntax"),
    GRAPH_PROJECT_CYPHER("graph-project-cypher-syntax"),
    GRAPH_PROJECT_CYPHER_AGGREGATION("graph-project-cypher-aggregation-syntax"),
    STREAM_NODE_PROPERTIES("stream-node-properties-syntax"),
    STREAM_SINGLE_PROPERTY("include-with-stream-single-property"),

    STREAM_MULTIPLE_PROPERTIES("include-with-stream-multiple-properties"),
    CONVERT_TO_UNDIRECTED("include-with-convert-to-undirected"),
    STREAM_TOPOLOGY("include-with-stream-topology"),
    GRAPH_EXISTS("graph-exists-syntax"),
    MODEL_EXISTS("model-exists-syntax"),
    GRAPH_EXPORT("graph-export-syntax"),
    PROJECT_SAMPLE("project-sample-syntax"),
    PROJECT_SUBGRAPH("project-subgraph-syntax"),
    REMOVE("include-with-remove"),
    DELETE_RELATIONSHIPS("include-with-delete-relationships"),
    WRITE_RELATIONSHIP_PROPERTIES("include-with-write-multiple-properties"),
    GRAPH_LIST("graph-list-syntax"),
    MODEL_LIST("model-list-syntax"),
    MODEL_PUBLISH("model-publish-syntax"),
    MODEL_STORE("model-store-syntax"),
    MODEL_LOAD("model-load-syntax"),
    MODEL_DELETE("model-delete-syntax"),
    PIPELINE_CREATE("pipeline-create-syntax"),
    PIPELINE_ADD_NODE_PROPERTY("pipeline-add-node-property-syntax"),
    PIPELINE_ADD_FEATURE("pipeline-add-feature-syntax"),
    PIPELINE_CONFIGURE_SPLIT("pipeline-configure-split-syntax"),
    PIPELINE_CONFIGURE_AUTO_TUNING("pipeline-configure-auto-tuning-syntax"),
    PIPELINE_ADD_LR_MODEL("pipeline-add-lr-syntax"),
    PIPELINE_ADD_RF_MODEL("pipeline-add-rf-syntax"),
    PIPELINE_CONFIGURE_PARAMS("pipeline-configure-params-syntax"),
    PIPELINE_LIST("pipeline-list-syntax"),
    PIPELINE_EXISTS("pipeline-exists-syntax"),
    PIPELINE_DROP("pipeline-drop-syntax"),
    LIST_PROGRESS("listProgress-syntax"),
    USER_LOG("userlog-syntax", false),
    SYSTEM_MONITOR("system-monitor-syntax", false),
    SYS_INFO("debug-sysinfo-syntax", false),
    WRITE_NODE_LABEL("include-with-write-node-label", false),
    MUTATE_NODE_LABEL("include-with-mutate-node-label", false),;

    private final String mode;
    public final boolean hasParameters;

    SyntaxMode(String mode) {
        this.mode = mode;
        this.hasParameters = true;
    }

    SyntaxMode(String mode, boolean hasParameters) {
        this.mode = mode;
        this.hasParameters = hasParameters;
    }

    public String mode() {
        return mode;
    }
}

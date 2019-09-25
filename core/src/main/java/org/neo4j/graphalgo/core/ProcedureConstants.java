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
package org.neo4j.graphalgo.core;

public final class ProcedureConstants {

    // used in graph.load and algos

    public static final String NODE_LABEL_QUERY_PARAM = "nodeQuery";
    public static final String RELATIONSHIP_QUERY_PARAM = "relationshipQuery";
    public static final String NODE_PROPERTY_PARAM = "nodeProperty";
    public static final String DEFAULT_VALUE_PARAM = "defaultValue";

    // graph.load specific

    public static final String NODE_WEIGHT_PARAM = "nodeWeight";
    public static final String RELATIONSHIP_WEIGHT_PARAM = "relationshipWeight";
    public static final String RELATIONSHIP_PROPERTIES = "relationshipProperties";

    // algos specific

    // graph type params
    public static final String GRAPH_IMPL_PARAM = "graph";
    public static final String GRAPH_IMPL_DEFAULT = "huge";
    public static final String DIRECTION_PARAM = "direction";
    public static final String UNDIRECTED_PARAM = "undirected";
    public static final String SORTED_PARAM = "sorted";
    public static final String CYPHER_QUERY_PARAM = "cypher";

    // write specific params
    public static final String WRITE_FLAG_PARAM = "write";
    public static final String WRITE_PROPERTY_PARAM = "writeProperty";
    public static final String WRITE_PROPERTY_DEFAULT = "writeValue";

    // concurrency related params
    public static final String BATCH_SIZE_PARAM = "batchSize";
    public static final String CONCURRENCY_PARAM = "concurrency";
    public static final String READ_CONCURRENCY_PARAM = "readConcurrency";
    public static final String WRITE_CONCURRENCY_PARAM = "writeConcurrency";

    // computation specific params
    public static final String ITERATIONS_PARAM = "iterations";

    // refers to the relationship weight to be used in weighted algorithms
    // that property is also considered in graph.load, despite being documented
    public static final String WEIGHT_PROPERTY_PARAM = "weightProperty";

    // BetweenessCentrality specific
    public static final String STATS_FLAG_PARAM = "stats";
    // ANN specific
    public static final String SKIP_VALUE_PARAM = "skipValue";

    private ProcedureConstants() {}
}

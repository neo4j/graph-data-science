/*
 * Copyright (c) 2017 "Neo4j, Inc." <http://neo4j.com>
 *
 * This file is part of Neo4j Graph Algorithms <http://github.com/neo4j-contrib/neo4j-graph-algorithms>.
 *
 * Neo4j Graph Algorithms is free software: you can redistribute it and/or modify
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

/**
 * @author mknblch
 */
public class ProcedureConstants {

    public static final String CYPHER_QUERY = "cypher";

    public static final String NODE_LABEL_QUERY_PARAM = "nodeQuery";

    public static final String RELATIONSHIP_QUERY_PARAM = "relationshipQuery";

    public static final String PROPERTY_PARAM = "weightProperty";

    public static final String PROPERTY_PARAM_DEFAULT = "weight";

    public static final String DEFAULT_PROPERTY_VALUE_PARAM = "defaultValue";

    public static final String WRITE_FLAG = "write";

    public static final String WRITE_PROPERTY = "writeProperty";

    public static final String WRITE_PROPERTY_DEFAULT = "writeValue";

    public static final String STATS_FLAG = "stats";

    public static final double DEFAULT_PROPERTY_VALUE_DEFAULT = 1.0;

    public static final String ITERATIONS_PARAM = "iterations";

    public static final int ITERATIONS_DEFAULT = 1;

    public static final String BATCH_SIZE_PARAM = "batchSize";

    public static final String DIRECTION = "direction";

    public static final String DIRECTION_DEFAULT = "BOTH";

    public static final String GRAPH_IMPL_PARAM = "graph";

    public static final String DEFAULT_GRAPH_IMPL = "heavy";

    public static final String CONCURRENCY = "concurrency";

}

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

    public static final String NODE_LABEL_QUERY_KEY = "nodeQuery";
    public static final String RELATIONSHIP_QUERY_KEY = "relationshipQuery";
    public static final String DEFAULT_VALUE_KEY = "defaultValue";
    public static final double DEFAULT_VALUE_DEFAULT = 1.0;

    // graph.load specific

    public static final String NODE_PROPERTIES_KEY = "nodeProperties";
    public static final String RELATIONSHIP_WEIGHT_KEY = "relationshipWeight";
    public static final String RELATIONSHIP_PROPERTIES_KEY = "relationshipProperties";

    // graph.generate specific

    // also refers to the relationship property  to be used in algorithms that make use of a property
    //  - most often interpreted as a weight of some sort.
    //  That property is also considered in graph.load, despite being documented
    public static final String RELATIONSHIP_PROPERTY_KEY = "relationshipProperty";

    public static final String RELATIONSHIP_DISTRIBUTION_KEY = "relationshipDistribution";
    public static final String RELATIONSHIP_PROPERTY_NAME_KEY = "name";
    public static final String RELATIONSHIP_PROPERTY_TYPE_KEY = "type";
    public static final String RELATIONSHIP_PROPERTY_MIN_KEY = "min";
    public static final String RELATIONSHIP_PROPERTY_MAX_KEY = "max";
    public static final String RELATIONSHIP_PROPERTY_VALUE_KEY = "value";
    public static final String RELATIONSHIP_PROPERTY_SEED = "seed";

    // algos specific
    public static final String SEED_PROPERTY_KEY = "seedProperty";

    public static final String TOLERANCE_KEY = "tolerance";
    public static final double TOLERANCE_DEFAULT = 0.0001D;

    // graph type params
    public static final String GRAPH_IMPL_KEY = "graph";
    public static final String GRAPH_IMPL_DEFAULT = "huge";
    public static final String DIRECTION_KEY = "direction";
    public static final String UNDIRECTED_KEY = "undirected";
    public static final String SORTED_KEY = "sorted";
    public static final String CYPHER_QUERY_KEY = "cypher";

    // write specific params
    public static final String WRITE_FLAG_KEY = "write";
    public static final String WRITE_PROPERTY_KEY = "writeProperty";
    public static final String WRITE_PROPERTY_DEFAULT = "writeValue";

    // concurrency related params
    public static final String BATCH_SIZE_KEY = "batchSize";
    public static final String CONCURRENCY_KEY = "concurrency";
    public static final String READ_CONCURRENCY_KEY = "readConcurrency";
    public static final String WRITE_CONCURRENCY_KEY = "writeConcurrency";

    // computation specific params
    public static final String ITERATIONS_KEY = "iterations";

    // refers to the relationship weight to be used in weighted algorithms
    // that property is also considered in graph.load, despite being documented
    // Deprecated: use RELATIONSHIP_PROPERTY_KEY instead
    @Deprecated
    public static final String DEPRECATED_RELATIONSHIP_PROPERTY_KEY = "weightProperty";

    // BetweenessCentrality specific
    public static final String STATS_FLAG_KEY = "stats";
    // ANN specific
    public static final String SKIP_VALUE_KEY = "skipValue";
    // PageRank specific
    public static final String DAMPING_FACTOR_KEY = "dampingFactor";
    public static final Double DAMPING_FACTOR_DEFAULT = 0.85D;

    private ProcedureConstants() {}
}

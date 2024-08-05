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
package org.neo4j.gds.procedures.catalog;

import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.config.GraphProjectConfig;

import java.util.Map;

@SuppressWarnings("unused")
public class GraphInfoWithHistogram extends GraphInfo {

    public final Map<String, Object> degreeDistribution;

    public GraphInfoWithHistogram(
        GraphInfo graphInfo,
        Map<String, Object> degreeDistribution
    ) {
        super(
            graphInfo.graphName,
            graphInfo.database,
            graphInfo.databaseLocation,
            graphInfo.configuration,
            graphInfo.memoryUsage,
            graphInfo.sizeInBytes,
            graphInfo.nodeCount,
            graphInfo.relationshipCount,
            graphInfo.creationTime,
            graphInfo.modificationTime,
            graphInfo.schema,
            graphInfo.schemaWithOrientation
        );
        this.degreeDistribution = degreeDistribution;
    }

    /**
     * @param degreeDistribution null implies not including it
     * @param computeGraphSize   selects what kind of GraphInfo to create
     */
    public static GraphInfoWithHistogram of(
        GraphProjectConfig graphProjectConfig,
        GraphStore graphStore,
        Map<String, Object> degreeDistribution,
        boolean computeGraphSize
    ) {
        var graphInfo = computeGraphSize
            ? withMemoryUsage(graphProjectConfig, graphStore)
            : withoutMemoryUsage(graphProjectConfig, graphStore);

        return new GraphInfoWithHistogram(graphInfo, degreeDistribution);
    }
}

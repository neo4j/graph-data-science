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
package org.neo4j.gds.catalog;

import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.config.GraphProjectConfig;
import org.neo4j.gds.config.GraphProjectFromCypherConfig;
import org.neo4j.gds.config.GraphProjectFromGraphConfig;
import org.neo4j.gds.config.GraphProjectFromStoreConfig;
import org.neo4j.gds.config.RandomGraphGeneratorConfig;
import org.neo4j.gds.mem.MemoryUsage;

import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.neo4j.gds.catalog.GraphInfoHelper.density;

public class GraphInfo {

    public final String graphName;
    public final String database;
    public final String memoryUsage;
    public final long sizeInBytes;
    public final long nodeCount;
    public final long relationshipCount;
    public final Map<String, Object> configuration;
    public final double density;
    public final ZonedDateTime creationTime;
    public final ZonedDateTime modificationTime;
    public final Map<String, Object> schema;

    GraphInfo(
        String graphName,
        String database,
        Map<String, Object> configuration,
        String memoryUsage,
        long sizeInBytes,
        long nodeCount,
        long relationshipCount,
        ZonedDateTime creationTime,
        ZonedDateTime modificationTime,
        Map<String, Object> schema
    ) {
        this.graphName = graphName;
        this.database = database;
        this.memoryUsage = memoryUsage;
        this.sizeInBytes = sizeInBytes;
        this.nodeCount = nodeCount;
        this.relationshipCount = relationshipCount;
        this.density = density(nodeCount, relationshipCount);
        this.creationTime = creationTime;
        this.modificationTime = modificationTime;
        this.schema = schema;
        this.configuration = configuration;
    }

    static GraphInfo withMemoryUsage(
        GraphProjectConfig graphProjectConfig,
        GraphStore graphStore,
        Optional<Set<String>> whiteListedKeysForOutput
    ) {
        var sizeInBytes = MemoryUsage.sizeOf(graphStore);
        var memoryUsage = MemoryUsage.humanReadable(sizeInBytes);

        return create(
            graphProjectConfig,
            graphStore,
            memoryUsage,
            sizeInBytes,
            whiteListedKeysForOutput
        );
    }

    static GraphInfo withoutMemoryUsage(
        GraphProjectConfig graphProjectConfig,
        GraphStore graphStore,
        Optional<Set<String>> whiteListedKeys
    ) {
        return create(
            graphProjectConfig,
            graphStore,
            "",
            -1L,
            whiteListedKeys
        );
    }

    private static GraphInfo create(
        GraphProjectConfig graphProjectConfig,
        GraphStore graphStore,
        String memoryUsage,
        long sizeInBytes,
        Optional<Set<String>> whiteListedKeys
    ) {
        var configVisitor = new Visitor();
        graphProjectConfig.accept(configVisitor);

        return new GraphInfo(
            graphProjectConfig.graphName(),
            graphStore.databaseId().name(),
            configVisitor.configuration,
            memoryUsage,
            sizeInBytes,
            graphStore.nodeCount(),
            graphStore.relationshipCount(),
            graphProjectConfig.creationTime(),
            graphStore.modificationTime(),
            graphStore.schema().toMap()
        );
    }

    static final class Visitor implements GraphProjectConfig.Visitor {

        Map<String, Object> configuration = null;

        @Override
        public void visit(GraphProjectFromStoreConfig storeConfig) {
            configuration = cleansed(storeConfig.toMap(), Set.of());
        }

        @Override
        public void visit(GraphProjectFromCypherConfig cypherConfig) {
            configuration = cleansed(cypherConfig.toMap(), cypherConfig.outputFieldDenylist());
        }

        @Override
        public void visit(GraphProjectFromGraphConfig graphConfig) {
            graphConfig.originalConfig().accept(this);
            configuration.putAll(graphConfig.toMap());
            configuration.put("nodeFilter", graphConfig.nodeFilter());
            configuration.put("relationshipFilter", graphConfig.relationshipFilter());
        }

        @Override
        public void visit(RandomGraphGeneratorConfig randomGraphConfig) {
            configuration = cleansed(randomGraphConfig.toMap(), Set.of());
            configuration.put("aggregation", randomGraphConfig.aggregation().toString());
            configuration.put("orientation", randomGraphConfig.orientation().toString());
            configuration.put("relationshipDistribution", randomGraphConfig.relationshipDistribution().toString());
        }

        private Map<String, Object> cleansed(Map<String, Object> map, Set<String> keysToIgnore) {
            Map<String, Object> result = new HashMap<>(map);
            map.forEach((key, value) -> {
                if (keysToIgnore.contains(key)) {
                    result.remove(key);
                }
            });
            return result;
        }
    }
}

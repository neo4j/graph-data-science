/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
package org.neo4j.graphalgo.config;

import org.immutables.value.Value;
import org.neo4j.graphalgo.NodeLabel;
import org.neo4j.graphalgo.RelationshipType;
import org.neo4j.graphalgo.annotation.Configuration;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.neo4j.graphalgo.NodeLabel.ALL_NODES;
import static org.neo4j.graphalgo.RelationshipType.ALL_RELATIONSHIPS;

public interface AlgoBaseConfig extends BaseConfig {

    int DEFAULT_CONCURRENCY = 4;
    String NODE_LABELS_KEY = "nodeLabels";
    List<String> ALL_NODE_LABELS = Collections.singletonList(ALL_NODES.name());
    List<NodeLabel> ALL_NODE_LABEL_IDENTIFIERS = Collections.singletonList(ALL_NODES);
    List<RelationshipType> ALL_RELATIONSHIP_TYPE_IDENTIFIERS = Collections.singletonList(ALL_RELATIONSHIPS);

    @Value.Default
    default int concurrency() {
        return DEFAULT_CONCURRENCY;
    }

    @Configuration.Parameter
    Optional<String> graphName();

    @Value.Default
    default List<String> relationshipTypes() {
        return Collections.singletonList("*");
    }

    @Configuration.Ignore
    default List<RelationshipType> relationshipTypeIdentifiers() {
        return Collections.singletonList(ALL_RELATIONSHIPS);
    }

    @Value.Default
    default List<String> nodeLabels() {
        return ALL_NODE_LABELS;
    }

    @Configuration.Ignore
    default List<NodeLabel> nodeLabelIdentifiers() {
        return nodeLabels().stream().map(NodeLabel::of).collect(Collectors.toList());
    }

    @Configuration.Parameter
    Optional<GraphCreateConfig> implicitCreateConfig();

}

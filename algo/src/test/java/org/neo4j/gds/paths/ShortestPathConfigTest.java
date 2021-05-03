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
package org.neo4j.gds.paths;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.paths.dijkstra.config.ShortestPathDijkstraStreamConfigImpl;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;

import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ShortestPathConfigTest {

    @Test
    void shouldAllowNodes() {
        var cypherMapWrapper = CypherMapWrapper
            .empty()
            .withEntry("sourceNode", new TestNode(42L))
            .withEntry("targetNode", new TestNode(1337L));

        var config = new ShortestPathDijkstraStreamConfigImpl(Optional.of("graph"), Optional.empty(), "", cypherMapWrapper);

        assertThat(config.sourceNode()).isEqualTo(42L);
        assertThat(config.targetNode()).isEqualTo(1337L);
    }

    @Test
    void shouldAllowNodeIds() {
        var cypherMapWrapper = CypherMapWrapper
            .empty()
            .withEntry("sourceNode", 42L)
            .withEntry("targetNode", 1337L);

        var config = new ShortestPathDijkstraStreamConfigImpl(Optional.of("graph"), Optional.empty(), "", cypherMapWrapper);

        assertThat(config.sourceNode()).isEqualTo(42L);
        assertThat(config.targetNode()).isEqualTo(1337L);
    }

    @Test
    void shouldThrowErrorOnUnsupportedType() {
        var cypherMapWrapper = CypherMapWrapper
            .empty()
            .withEntry("sourceNode", "42")
            .withEntry("targetNode", false);

        assertThatThrownBy(() -> new ShortestPathDijkstraStreamConfigImpl(Optional.of("graph"), Optional.empty(), "", cypherMapWrapper))
            .hasMessageContaining("Expected a node or a node id for `sourceNode`. Got String")
            .hasMessageContaining("Expected a node or a node id for `targetNode`. Got Boolean");
    }

    static final class TestNode implements Node {
        private final long id;

        TestNode(long id) {this.id = id;}

        @Override
        public void delete() {

        }

        @Override
        public Iterable<Relationship> getRelationships() {
            return null;
        }

        @Override
        public boolean hasRelationship() {
            return false;
        }

        @Override
        public Iterable<Relationship> getRelationships(RelationshipType... types) {
            return null;
        }

        @Override
        public Iterable<Relationship> getRelationships(
            Direction direction, RelationshipType... types
        ) {
            return null;
        }

        @Override
        public boolean hasRelationship(RelationshipType... types) {
            return false;
        }

        @Override
        public boolean hasRelationship(Direction direction, RelationshipType... types) {
            return false;
        }

        @Override
        public Iterable<Relationship> getRelationships(Direction dir) {
            return null;
        }

        @Override
        public boolean hasRelationship(Direction dir) {
            return false;
        }

        @Override
        public Relationship getSingleRelationship(
            RelationshipType type, Direction dir
        ) {
            return null;
        }

        @Override
        public Relationship createRelationshipTo(Node otherNode, RelationshipType type) {
            return null;
        }

        @Override
        public Iterable<RelationshipType> getRelationshipTypes() {
            return null;
        }

        @Override
        public int getDegree() {
            return 0;
        }

        @Override
        public int getDegree(RelationshipType type) {
            return 0;
        }

        @Override
        public int getDegree(Direction direction) {
            return 0;
        }

        @Override
        public int getDegree(RelationshipType type, Direction direction) {
            return 0;
        }

        @Override
        public void addLabel(Label label) {

        }

        @Override
        public void removeLabel(Label label) {

        }

        @Override
        public boolean hasLabel(Label label) {
            return false;
        }

        @Override
        public Iterable<Label> getLabels() {
            return null;
        }

        @Override
        public long getId() {
            return id;
        }

        @Override
        public boolean hasProperty(String key) {
            return false;
        }

        @Override
        public Object getProperty(String key) {
            return null;
        }

        @Override
        public Object getProperty(String key, Object defaultValue) {
            return null;
        }

        @Override
        public void setProperty(String key, Object value) {

        }

        @Override
        public Object removeProperty(String key) {
            return null;
        }

        @Override
        public Iterable<String> getPropertyKeys() {
            return null;
        }

        @Override
        public Map<String, Object> getProperties(String... keys) {
            return null;
        }

        @Override
        public Map<String, Object> getAllProperties() {
            return null;
        }
    }
}

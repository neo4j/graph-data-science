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
package org.neo4j.gds.algorithms;

import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.api.User;
import org.neo4j.gds.core.utils.progress.TaskRegistryFactory;
import org.neo4j.gds.core.utils.warnings.UserLogRegistryFactory;
import org.neo4j.gds.core.write.NodePropertyExporterBuilder;
import org.neo4j.gds.core.write.RelationshipExporterBuilder;
import org.neo4j.gds.core.write.RelationshipStreamExporterBuilder;
import org.neo4j.gds.termination.TerminationFlag;

/**
 * This is a handy class for transporting similarly scoped dependencies through layers.
 * And especially useful when that list grows or shrinks - less sites to edit innit.
 */
public final class RequestScopedDependencies {
    private final DatabaseId databaseId;
    private final NodePropertyExporterBuilder nodePropertyExporterBuilder;
    private final RelationshipExporterBuilder relationshipExporterBuilder;
    private final RelationshipStreamExporterBuilder relationshipStreamExporterBuilder;
    private final TaskRegistryFactory taskRegistryFactory;
    private final TerminationFlag terminationFlag;
    private final User user;
    private final UserLogRegistryFactory userLogRegistryFactory;

    /**
     * Over-doing it with a private constructor?
     * <p>
     * I just really like the <code>RequestScopedDependencies.builder().build()</code> form
     */
    private RequestScopedDependencies(
        DatabaseId databaseId,
        NodePropertyExporterBuilder nodePropertyExporterBuilder,
        RelationshipExporterBuilder relationshipExporterBuilder,
        RelationshipStreamExporterBuilder relationshipStreamExporterBuilder,
        TaskRegistryFactory taskRegistryFactory,
        TerminationFlag terminationFlag,
        User user,
        UserLogRegistryFactory userLogRegistryFactory
    ) {
        this.databaseId = databaseId;
        this.nodePropertyExporterBuilder = nodePropertyExporterBuilder;
        this.relationshipExporterBuilder = relationshipExporterBuilder;
        this.relationshipStreamExporterBuilder = relationshipStreamExporterBuilder;
        this.taskRegistryFactory = taskRegistryFactory;
        this.terminationFlag = terminationFlag;
        this.user = user;
        this.userLogRegistryFactory = userLogRegistryFactory;
    }

    public static RequestScopedDependenciesBuilder builder() {
        return new RequestScopedDependenciesBuilder();
    }

    public DatabaseId getDatabaseId() {
        return databaseId;
    }

    public NodePropertyExporterBuilder getNodePropertyExporterBuilder() {
        return nodePropertyExporterBuilder;
    }

    public RelationshipExporterBuilder getRelationshipExporterBuilder() {
        return relationshipExporterBuilder;
    }

    public RelationshipStreamExporterBuilder getRelationshipStreamExporterBuilder() {
        return relationshipStreamExporterBuilder;
    }

    public TaskRegistryFactory getTaskRegistryFactory() {
        return taskRegistryFactory;
    }

    public TerminationFlag getTerminationFlag() {
        return terminationFlag;
    }

    public User getUser() {
        return user;
    }

    public UserLogRegistryFactory getUserLogRegistryFactory() {
        return userLogRegistryFactory;
    }

    /**
     * A handy builder where you can include as many or as few components as you are interested in.
     * We deliberately do not have defaults,
     * because trying to reconcile convenience across all usages is an error-prone form of coupling.
     */
    public static class RequestScopedDependenciesBuilder {
        private DatabaseId databaseId;
        private NodePropertyExporterBuilder nodePropertyExporterBuilder;
        private RelationshipExporterBuilder relationshipExporterBuilder;
        private RelationshipStreamExporterBuilder relationshipStreamExporterBuilder;
        private TerminationFlag terminationFlag;
        private TaskRegistryFactory taskRegistryFactory;
        private User user;
        private UserLogRegistryFactory userLogRegistryFactory;

        public RequestScopedDependenciesBuilder with(DatabaseId databaseId) {
            this.databaseId = databaseId;
            return this;
        }

        public RequestScopedDependenciesBuilder with(NodePropertyExporterBuilder nodePropertyExporterBuilder) {
            this.nodePropertyExporterBuilder = nodePropertyExporterBuilder;
            return this;
        }

        public RequestScopedDependenciesBuilder with(RelationshipExporterBuilder relationshipExporterBuilder) {
            this.relationshipExporterBuilder = relationshipExporterBuilder;
            return this;
        }

        public RequestScopedDependenciesBuilder with(RelationshipStreamExporterBuilder relationshipStreamExporterBuilder) {
            this.relationshipStreamExporterBuilder = relationshipStreamExporterBuilder;
            return this;
        }

        public RequestScopedDependenciesBuilder with(TaskRegistryFactory taskRegistryFactory) {
            this.taskRegistryFactory = taskRegistryFactory;
            return this;
        }

        public RequestScopedDependenciesBuilder with(TerminationFlag terminationFlag) {
            this.terminationFlag = terminationFlag;
            return this;
        }

        public RequestScopedDependenciesBuilder with(User user) {
            this.user = user;
            return this;
        }

        public RequestScopedDependenciesBuilder with(UserLogRegistryFactory userLogRegistryFactory) {
            this.userLogRegistryFactory = userLogRegistryFactory;
            return this;
        }

        public RequestScopedDependencies build() {
            return new RequestScopedDependencies(
                databaseId,
                nodePropertyExporterBuilder,
                relationshipExporterBuilder,
                relationshipStreamExporterBuilder,
                taskRegistryFactory,
                terminationFlag,
                user,
                userLogRegistryFactory
            );
        }
    }
}

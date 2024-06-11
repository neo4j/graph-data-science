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
package org.neo4j.gds.applications.algorithms.machinery;

import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.api.GraphLoaderContext;
import org.neo4j.gds.api.ProcedureReturnColumns;
import org.neo4j.gds.api.User;
import org.neo4j.gds.core.utils.progress.TaskRegistryFactory;
import org.neo4j.gds.core.utils.warnings.UserLogRegistryFactory;
import org.neo4j.gds.core.utils.warnings.UserLogStore;
import org.neo4j.gds.core.write.NodeLabelExporterBuilder;
import org.neo4j.gds.core.write.NodePropertyExporterBuilder;
import org.neo4j.gds.core.write.RelationshipExporterBuilder;
import org.neo4j.gds.core.write.RelationshipPropertiesExporterBuilder;
import org.neo4j.gds.core.write.RelationshipStreamExporterBuilder;
import org.neo4j.gds.termination.TerminationFlag;

/**
 * This is a handy class for transporting similarly scoped dependencies through layers.
 * And especially useful when that list grows or shrinks - fewer sites to edit innit.
 */
public final class RequestScopedDependencies {
    private final DatabaseId databaseId;
    private final GraphLoaderContext graphLoaderContext;
    private final NodeLabelExporterBuilder nodeLabelExporterBuilder;
    private final NodePropertyExporterBuilder nodePropertyExporterBuilder;
    private final ProcedureReturnColumns procedureReturnColumns;
    private final RelationshipExporterBuilder relationshipExporterBuilder;
    private final RelationshipPropertiesExporterBuilder relationshipPropertiesExporterBuilder;
    private final RelationshipStreamExporterBuilder relationshipStreamExporterBuilder;
    private final TaskRegistryFactory taskRegistryFactory;
    private final TerminationFlag terminationFlag;
    private final User user;
    private final UserLogRegistryFactory userLogRegistryFactory;
    private final UserLogStore userLogStore;

    /**
     * Over-doing it with a private constructor?
     * <p>
     * I just really like the <code>RequestScopedDependencies.builder().build()</code> form
     */
    private RequestScopedDependencies(
        DatabaseId databaseId,
        GraphLoaderContext graphLoaderContext,
        NodeLabelExporterBuilder nodeLabelExporterBuilder,
        NodePropertyExporterBuilder nodePropertyExporterBuilder,
        ProcedureReturnColumns procedureReturnColumns,
        RelationshipExporterBuilder relationshipExporterBuilder,
        RelationshipPropertiesExporterBuilder relationshipPropertiesExporterBuilder,
        RelationshipStreamExporterBuilder relationshipStreamExporterBuilder,
        TaskRegistryFactory taskRegistryFactory,
        TerminationFlag terminationFlag,
        User user,
        UserLogRegistryFactory userLogRegistryFactory,
        UserLogStore userLogStore
    ) {
        this.databaseId = databaseId;
        this.graphLoaderContext = graphLoaderContext;
        this.nodeLabelExporterBuilder = nodeLabelExporterBuilder;
        this.nodePropertyExporterBuilder = nodePropertyExporterBuilder;
        this.procedureReturnColumns = procedureReturnColumns;
        this.relationshipExporterBuilder = relationshipExporterBuilder;
        this.relationshipPropertiesExporterBuilder = relationshipPropertiesExporterBuilder;
        this.relationshipStreamExporterBuilder = relationshipStreamExporterBuilder;
        this.taskRegistryFactory = taskRegistryFactory;
        this.terminationFlag = terminationFlag;
        this.user = user;
        this.userLogRegistryFactory = userLogRegistryFactory;
        this.userLogStore = userLogStore;
    }

    public static RequestScopedDependenciesBuilder builder() {
        return new RequestScopedDependenciesBuilder();
    }

    public DatabaseId getDatabaseId() {
        return databaseId;
    }

    public GraphLoaderContext getGraphLoaderContext() {
        return graphLoaderContext;
    }

    public NodeLabelExporterBuilder getNodeLabelExporterBuilder() {
        return nodeLabelExporterBuilder;
    }

    public NodePropertyExporterBuilder getNodePropertyExporterBuilder() {
        return nodePropertyExporterBuilder;
    }

    public ProcedureReturnColumns getProcedureReturnColumns() {
        return procedureReturnColumns;
    }

    public RelationshipExporterBuilder getRelationshipExporterBuilder() {
        return relationshipExporterBuilder;
    }

    public RelationshipPropertiesExporterBuilder getRelationshipPropertiesExporterBuilder() {
        return relationshipPropertiesExporterBuilder;
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

    public UserLogStore getUserLogStore() {
        return userLogStore;
    }

    /**
     * A handy builder where you can include as many or as few components as you are interested in.
     * We deliberately do not have defaults,
     * because trying to reconcile convenience across all usages is an error-prone form of coupling.
     */
    public static class RequestScopedDependenciesBuilder {
        private DatabaseId databaseId;
        private GraphLoaderContext graphLoaderContext;
        private NodeLabelExporterBuilder nodeLabelExporterBuilder;
        private NodePropertyExporterBuilder nodePropertyExporterBuilder;
        private ProcedureReturnColumns procedureReturnColumns;
        private RelationshipExporterBuilder relationshipExporterBuilder;
        private RelationshipPropertiesExporterBuilder relationshipPropertiesExporterBuilder;
        private RelationshipStreamExporterBuilder relationshipStreamExporterBuilder;
        private TerminationFlag terminationFlag;
        private TaskRegistryFactory taskRegistryFactory;
        private User user;
        private UserLogRegistryFactory userLogRegistryFactory;
        private UserLogStore userLogStore;

        public RequestScopedDependenciesBuilder with(DatabaseId databaseId) {
            this.databaseId = databaseId;
            return this;
        }

        public RequestScopedDependenciesBuilder with(GraphLoaderContext graphLoaderContext) {
            this.graphLoaderContext = graphLoaderContext;
            return this;
        }

        public RequestScopedDependenciesBuilder with(NodeLabelExporterBuilder nodeLabelExporterBuilder) {
            this.nodeLabelExporterBuilder = nodeLabelExporterBuilder;
            return this;
        }

        public RequestScopedDependenciesBuilder with(NodePropertyExporterBuilder nodePropertyExporterBuilder) {
            this.nodePropertyExporterBuilder = nodePropertyExporterBuilder;
            return this;
        }

        public RequestScopedDependenciesBuilder with(ProcedureReturnColumns procedureReturnColumns) {
            this.procedureReturnColumns = procedureReturnColumns;
            return this;
        }

        public RequestScopedDependenciesBuilder with(RelationshipExporterBuilder relationshipExporterBuilder) {
            this.relationshipExporterBuilder = relationshipExporterBuilder;
            return this;
        }

        public RequestScopedDependenciesBuilder with(RelationshipPropertiesExporterBuilder relationshipPropertiesExporterBuilder) {
            this.relationshipPropertiesExporterBuilder = relationshipPropertiesExporterBuilder;
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

        public RequestScopedDependenciesBuilder with(UserLogStore userLogStore) {
            this.userLogStore = userLogStore;
            return this;
        }

        public RequestScopedDependencies build() {
            return new RequestScopedDependencies(
                databaseId,
                graphLoaderContext,
                nodeLabelExporterBuilder,
                nodePropertyExporterBuilder,
                procedureReturnColumns,
                relationshipExporterBuilder,
                relationshipPropertiesExporterBuilder,
                relationshipStreamExporterBuilder,
                taskRegistryFactory,
                terminationFlag,
                user,
                userLogRegistryFactory,
                userLogStore
            );
        }
    }
}

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
package org.neo4j.gds.executor;

import org.jetbrains.annotations.Nullable;
import org.neo4j.common.DependencyResolver;
import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.api.AlgorithmMetaDataSetter;
import org.neo4j.gds.api.CloseableResourceRegistry;
import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.api.EmptyDependencyResolver;
import org.neo4j.gds.api.NodeLookup;
import org.neo4j.gds.api.ProcedureReturnColumns;
import org.neo4j.gds.api.TerminationMonitor;
import org.neo4j.gds.core.model.ModelCatalog;
import org.neo4j.gds.core.utils.progress.EmptyTaskRegistryFactory;
import org.neo4j.gds.core.utils.progress.TaskRegistryFactory;
import org.neo4j.gds.core.utils.warnings.EmptyUserLogRegistryFactory;
import org.neo4j.gds.core.utils.warnings.UserLogRegistryFactory;
import org.neo4j.gds.core.write.NodePropertyExporterBuilder;
import org.neo4j.gds.core.write.RelationshipExporterBuilder;
import org.neo4j.gds.core.write.RelationshipStreamExporterBuilder;
import org.neo4j.logging.Log;
import org.neo4j.logging.NullLog;

@ValueClass
public interface ExecutionContext {

    DatabaseId databaseId();

    DependencyResolver dependencyResolver();

    ModelCatalog modelCatalog();

    Log log();

    TerminationMonitor terminationMonitor();

    CloseableResourceRegistry closeableResourceRegistry();

    AlgorithmMetaDataSetter algorithmMetaDataSetter();

    NodeLookup nodeLookup();

    ProcedureReturnColumns returnColumns();

    TaskRegistryFactory taskRegistryFactory();

    UserLogRegistryFactory userLogRegistryFactory();

    String username();

    boolean isGdsAdmin();

    @Nullable
    RelationshipStreamExporterBuilder relationshipStreamExporterBuilder();

    @Nullable
    RelationshipExporterBuilder relationshipExporterBuilder();

    @Nullable
    NodePropertyExporterBuilder nodePropertyExporterBuilder();

    default ExecutionContext withNodePropertyExporterBuilder(NodePropertyExporterBuilder nodePropertyExporterBuilder) {
        return ImmutableExecutionContext
            .builder()
            .from(this)
            .nodePropertyExporterBuilder(nodePropertyExporterBuilder)
            .build();
    }

    default ExecutionContext withRelationshipStreamExporterBuilder(RelationshipStreamExporterBuilder relationshipStreamExporterBuilder) {
        return ImmutableExecutionContext
            .builder()
            .from(this)
            .relationshipStreamExporterBuilder(relationshipStreamExporterBuilder)
            .build();
    }

    default ExecutionContext withRelationshipExporterBuilder(RelationshipExporterBuilder relationshipExporterBuilder) {
        return ImmutableExecutionContext
            .builder()
            .from(this)
            .relationshipExporterBuilder(relationshipExporterBuilder)
            .build();
    }

    ExecutionContext EMPTY = new ExecutionContext() {

        @Override
        public DatabaseId databaseId() {
            return DatabaseId.EMPTY;
        }

        @Override
        public DependencyResolver dependencyResolver() {
            return EmptyDependencyResolver.INSTANCE;
        }

        @Override
        public ModelCatalog modelCatalog() {
            return ModelCatalog.EMPTY;
        }

        @Override
        public Log log() {
            return NullLog.getInstance();
        }

        @Override
        public AlgorithmMetaDataSetter algorithmMetaDataSetter() {
            return AlgorithmMetaDataSetter.EMPTY;
        }

        @Override
        public TerminationMonitor terminationMonitor() {
            return TerminationMonitor.EMPTY;
        }

        @Override
        public CloseableResourceRegistry closeableResourceRegistry() {
            return CloseableResourceRegistry.EMPTY;
        }

        @Override
        public NodeLookup nodeLookup() {
            return NodeLookup.EMPTY;
        }

        @Override
        public ProcedureReturnColumns returnColumns() {
            return ProcedureReturnColumns.EMPTY;
        }

        @Override
        public TaskRegistryFactory taskRegistryFactory() {
            return EmptyTaskRegistryFactory.INSTANCE;
        }

        @Override
        public UserLogRegistryFactory userLogRegistryFactory() {
            return EmptyUserLogRegistryFactory.INSTANCE;
        }

        @Override
        public boolean isGdsAdmin() {
            return false;
        }

        @Override
        public @Nullable RelationshipStreamExporterBuilder relationshipStreamExporterBuilder() {
            return null;
        }

        @Override
        public @Nullable RelationshipExporterBuilder relationshipExporterBuilder() {
            return null;
        }

        @Override
        public @Nullable NodePropertyExporterBuilder nodePropertyExporterBuilder() {
            return null;
        }

        @Override
        public String username() {
            return "";
        }
    };
}

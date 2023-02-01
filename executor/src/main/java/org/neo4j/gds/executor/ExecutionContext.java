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

import org.immutables.value.Value;
import org.jetbrains.annotations.Nullable;
import org.neo4j.common.DependencyResolver;
import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.api.AlgorithmMetaDataSetter;
import org.neo4j.gds.api.CloseableResourceRegistry;
import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.api.EmptyDependencyResolver;
import org.neo4j.gds.api.NodeLookup;
import org.neo4j.gds.api.TerminationMonitor;
import org.neo4j.gds.core.model.ModelCatalog;
import org.neo4j.gds.core.utils.progress.EmptyTaskRegistryFactory;
import org.neo4j.gds.core.utils.progress.TaskRegistryFactory;
import org.neo4j.gds.core.utils.warnings.EmptyUserLogRegistryFactory;
import org.neo4j.gds.core.utils.warnings.UserLogRegistryFactory;
import org.neo4j.gds.core.write.NodePropertyExporter;
import org.neo4j.gds.core.write.NodePropertyExporterBuilder;
import org.neo4j.gds.core.write.RelationshipExporter;
import org.neo4j.gds.core.write.RelationshipExporterBuilder;
import org.neo4j.gds.core.write.RelationshipStreamExporter;
import org.neo4j.gds.core.write.RelationshipStreamExporterBuilder;
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;
import org.neo4j.logging.Log;
import org.neo4j.logging.NullLog;

import static org.neo4j.gds.utils.StringFormatting.toLowerCaseWithLocale;

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

    ProcedureCallContext callContext();

    TaskRegistryFactory taskRegistryFactory();

    UserLogRegistryFactory userLogRegistryFactory();

    String username();

    boolean isGdsAdmin();

    @Nullable
    RelationshipStreamExporterBuilder<? extends RelationshipStreamExporter> relationshipStreamExporterBuilder();

    @Nullable
    RelationshipExporterBuilder<? extends RelationshipExporter> relationshipExporterBuilder();

    @Nullable
    NodePropertyExporterBuilder<? extends NodePropertyExporter> nodePropertyExporterBuilder();

    @Value.Lazy
    default boolean containsOutputField(String fieldName) {
        return callContext().outputFields()
            .anyMatch(field -> toLowerCaseWithLocale(field).equals(fieldName));
    }

    default ExecutionContext withNodePropertyExporterBuilder(NodePropertyExporterBuilder<? extends NodePropertyExporter> nodePropertyExporterBuilder) {
        return ImmutableExecutionContext
            .builder()
            .from(this)
            .nodePropertyExporterBuilder(nodePropertyExporterBuilder)
            .build();
    }

    default ExecutionContext withRelationshipStreamExporterBuilder(RelationshipStreamExporterBuilder<? extends RelationshipStreamExporter> relationshipStreamExporterBuilder) {
        return ImmutableExecutionContext
            .builder()
            .from(this)
            .relationshipStreamExporterBuilder(relationshipStreamExporterBuilder)
            .build();
    }

    default ExecutionContext withRelationshipExporterBuilder(RelationshipExporterBuilder<? extends RelationshipExporter> relationshipExporterBuilder) {
        return ImmutableExecutionContext
            .builder()
            .from(this)
            .relationshipExporterBuilder(relationshipExporterBuilder)
            .build();
    }

    ExecutionContext EMPTY = new ExecutionContext() {

        @Override
        public @Nullable DatabaseId databaseId() {
            return null;
        }

        @Override
        public DependencyResolver dependencyResolver() {
            return EmptyDependencyResolver.INSTANCE;
        }

        @Override
        public @Nullable ModelCatalog modelCatalog() {
            return null;
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
        public ProcedureCallContext callContext() {
            return ProcedureCallContext.EMPTY;
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
        public @Nullable RelationshipStreamExporterBuilder<? extends RelationshipStreamExporter> relationshipStreamExporterBuilder() {
            return null;
        }

        @Override
        public @Nullable RelationshipExporterBuilder<? extends RelationshipExporter> relationshipExporterBuilder() {
            return null;
        }

        @Override
        public @Nullable NodePropertyExporterBuilder<? extends NodePropertyExporter> nodePropertyExporterBuilder() {
            return null;
        }

        @Override
        public String username() {
            return "";
        }
    };
}

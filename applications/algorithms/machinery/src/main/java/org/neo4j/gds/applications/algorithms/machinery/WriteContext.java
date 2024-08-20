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


import org.neo4j.gds.core.write.ExportBuildersProvider;
import org.neo4j.gds.core.write.ExporterContext;
import org.neo4j.gds.core.write.NodeLabelExporterBuilder;
import org.neo4j.gds.core.write.NodePropertyExporterBuilder;
import org.neo4j.gds.core.write.RelationshipExporterBuilder;
import org.neo4j.gds.core.write.RelationshipPropertiesExporterBuilder;
import org.neo4j.gds.core.write.RelationshipStreamExporterBuilder;

/**
 * The parameter object for all things write.
 * Not included in {@link org.neo4j.gds.applications.algorithms.machinery.RequestScopedDependencies},
 * because we could want to isolate the write code paths, since they are all Neo4j Procedures specific.
 */
public record WriteContext(
    NodeLabelExporterBuilder nodeLabelExporterBuilder,
    NodePropertyExporterBuilder nodePropertyExporterBuilder,
    RelationshipExporterBuilder relationshipExporterBuilder,
    RelationshipPropertiesExporterBuilder relationshipPropertiesExporterBuilder,
    RelationshipStreamExporterBuilder relationshipStreamExporterBuilder) {

    public static WriteContext create(ExportBuildersProvider exportBuildersProvider, ExporterContext exporterContext) {
        var nodeLabelExporterBuilder = exportBuildersProvider.nodeLabelExporterBuilder(exporterContext);
        var nodePropertyExporterBuilder = exportBuildersProvider.nodePropertyExporterBuilder(exporterContext);
        var relationshipExporterBuilder = exportBuildersProvider.relationshipExporterBuilder(exporterContext);
        var relationshipPropertiesExporterBuilder = exportBuildersProvider.relationshipPropertiesExporterBuilder(
            exporterContext);
        var relationshipStreamExporterBuilder = exportBuildersProvider.relationshipStreamExporterBuilder(exporterContext);

        return new WriteContext(
            nodeLabelExporterBuilder,
            nodePropertyExporterBuilder,
            relationshipExporterBuilder,
            relationshipPropertiesExporterBuilder,
            relationshipStreamExporterBuilder
        );
    }

    public static WriteContextBuilder builder() {
        return new WriteContextBuilder();
    }

    public static class WriteContextBuilder {
        private NodeLabelExporterBuilder nodeLabelExporterBuilder;
        private NodePropertyExporterBuilder nodePropertyExporterBuilder;
        private RelationshipExporterBuilder relationshipExporterBuilder;
        private RelationshipPropertiesExporterBuilder relationshipPropertiesExporterBuilder;
        private RelationshipStreamExporterBuilder relationshipStreamExporterBuilder;

        public WriteContextBuilder with(NodeLabelExporterBuilder nodeLabelExporterBuilder) {
            this.nodeLabelExporterBuilder = nodeLabelExporterBuilder;
            return this;
        }

        public WriteContextBuilder with(NodePropertyExporterBuilder nodePropertyExporterBuilder) {
            this.nodePropertyExporterBuilder = nodePropertyExporterBuilder;
            return this;
        }

        public WriteContextBuilder with(RelationshipExporterBuilder relationshipExporterBuilder) {
            this.relationshipExporterBuilder = relationshipExporterBuilder;
            return this;
        }

        public WriteContextBuilder with(RelationshipPropertiesExporterBuilder relationshipPropertiesExporterBuilder) {
            this.relationshipPropertiesExporterBuilder = relationshipPropertiesExporterBuilder;
            return this;
        }

        public WriteContextBuilder with(RelationshipStreamExporterBuilder relationshipStreamExporterBuilder) {
            this.relationshipStreamExporterBuilder = relationshipStreamExporterBuilder;
            return this;
        }

        public WriteContext build() {
            return new WriteContext(
                nodeLabelExporterBuilder,
                nodePropertyExporterBuilder,
                relationshipExporterBuilder,
                relationshipPropertiesExporterBuilder,
                relationshipStreamExporterBuilder
            );
        }
    }

}

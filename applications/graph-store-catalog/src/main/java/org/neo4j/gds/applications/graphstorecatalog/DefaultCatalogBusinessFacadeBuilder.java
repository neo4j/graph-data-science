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
package org.neo4j.gds.applications.graphstorecatalog;

import org.neo4j.gds.core.loading.GraphStoreCatalogService;
import org.neo4j.gds.logging.Log;
import org.neo4j.gds.metrics.projections.ProjectionMetricsService;



 class DefaultCatalogBusinessFacadeBuilder  {

    // global dependencies
    private  Log log;
    private  GraphStoreCatalogService graphStoreCatalogService;
    private  ProjectionMetricsService projectionMetricsService;

    // services
    private  GraphNameValidationService graphNameValidationService;

    // applications
    private  DropGraphApplication dropGraphApplication;
    private  ListGraphApplication listGraphApplication;
    private  NativeProjectApplication nativeProjectApplication;
    private  CypherProjectApplication cypherProjectApplication;
    private  SubGraphProjectApplication subGraphProjectApplication;
    private  GraphMemoryUsageApplication graphMemoryUsageApplication;
    private  DropNodePropertiesApplication dropNodePropertiesApplication;
    private  DropRelationshipsApplication dropRelationshipsApplication;
    private  NodeLabelMutatorApplication nodeLabelMutatorApplication;
    private  StreamNodePropertiesApplication streamNodePropertiesApplication;
    private  StreamRelationshipPropertiesApplication streamRelationshipPropertiesApplication;
    private  StreamRelationshipsApplication streamRelationshipsApplication;
    private  WriteNodePropertiesApplication writeNodePropertiesApplication;
    private  WriteRelationshipPropertiesApplication writeRelationshipPropertiesApplication;
    private  WriteNodeLabelApplication writeNodeLabelApplication;
    private  WriteRelationshipsApplication writeRelationshipsApplication;
    private  GraphSamplingApplication graphSamplingApplication;
    private  EstimateCommonNeighbourAwareRandomWalkApplication estimateCommonNeighbourAwareRandomWalkApplication;
    private  GenerateGraphApplication generateGraphApplication;

    DefaultCatalogBusinessFacadeBuilder withLog(Log log) {
        this.log = log;
        return  this;
    }

    DefaultCatalogBusinessFacadeBuilder withGraphStoreCatalogService(GraphStoreCatalogService graphStoreCatalogService) {
        this.graphStoreCatalogService = graphStoreCatalogService;
        return  this;
    }
    DefaultCatalogBusinessFacadeBuilder withProjectionMetricsService(ProjectionMetricsService projectionMetricsService) {
        this.projectionMetricsService = projectionMetricsService;
        return this;
    }
    DefaultCatalogBusinessFacadeBuilder withGraphNameValidationService(GraphNameValidationService graphNameValidationService) {
        this.graphNameValidationService = graphNameValidationService;
        return this;
    }
    DefaultCatalogBusinessFacadeBuilder withCypherProjectApplication(CypherProjectApplication cypherProjectApplication) {
        this.cypherProjectApplication = cypherProjectApplication;
        return this;
    }
    DefaultCatalogBusinessFacadeBuilder withDropGraphApplication(DropGraphApplication dropGraphApplication) {
        this.dropGraphApplication = dropGraphApplication;
        return this;
    }
    DefaultCatalogBusinessFacadeBuilder withDropNodePropertiesApplication(DropNodePropertiesApplication dropNodePropertiesApplication) {
        this.dropNodePropertiesApplication=dropNodePropertiesApplication;
        return this;
    }
    DefaultCatalogBusinessFacadeBuilder withDropRelationshipsApplication(DropRelationshipsApplication dropRelationshipsApplication) {
        this.dropRelationshipsApplication = dropRelationshipsApplication;
        return this;
    }

    DefaultCatalogBusinessFacadeBuilder withEstimateCommonNeighbourAwareRandomWalkApplication(EstimateCommonNeighbourAwareRandomWalkApplication estimateCommonNeighbourAwareRandomWalkApplication) {
        this.estimateCommonNeighbourAwareRandomWalkApplication = estimateCommonNeighbourAwareRandomWalkApplication;
        return this;
    }
    DefaultCatalogBusinessFacadeBuilder withGenerateGraphApplication(GenerateGraphApplication generateGraphApplication) {
        this.generateGraphApplication = generateGraphApplication;
        return  this;
    }
    DefaultCatalogBusinessFacadeBuilder withGraphMemoryUsageApplication(GraphMemoryUsageApplication graphMemoryUsageApplication) {
        this.graphMemoryUsageApplication= graphMemoryUsageApplication;
        return  this;
    }
    DefaultCatalogBusinessFacadeBuilder withGraphSamplingApplication(GraphSamplingApplication graphSamplingApplication) {
        this.graphSamplingApplication = graphSamplingApplication;
        return this;
    }

    DefaultCatalogBusinessFacadeBuilder withListGraphApplication(ListGraphApplication listGraphApplication) {
        this.listGraphApplication = listGraphApplication;
        return this;
    }

    DefaultCatalogBusinessFacadeBuilder withNativeProjectApplication(NativeProjectApplication nativeProjectApplication) {
        this.nativeProjectApplication = nativeProjectApplication;
        return  this;
    }

    DefaultCatalogBusinessFacadeBuilder withNodeLabelMutatorApplication(NodeLabelMutatorApplication nodeLabelMutatorApplication) {
        this.nodeLabelMutatorApplication = nodeLabelMutatorApplication;
        return  this;
    }

    DefaultCatalogBusinessFacadeBuilder withStreamNodePropertiesApplication(StreamNodePropertiesApplication streamNodePropertiesApplication) {
        this.streamNodePropertiesApplication = streamNodePropertiesApplication;
        return this;
    }

    DefaultCatalogBusinessFacadeBuilder withStreamRelationshipPropertiesApplication(StreamRelationshipPropertiesApplication streamRelationshipPropertiesApplication) {
        this.streamRelationshipPropertiesApplication = streamRelationshipPropertiesApplication;
        return this;
    }

    DefaultCatalogBusinessFacadeBuilder withStreamRelationshipsApplication(StreamRelationshipsApplication streamRelationshipsApplication) {
        this.streamRelationshipsApplication= streamRelationshipsApplication;
        return  this;
    }


    DefaultCatalogBusinessFacadeBuilder withSubGraphProjectApplication(SubGraphProjectApplication subGraphProjectApplication) {
        this.subGraphProjectApplication = subGraphProjectApplication;
        return this;
    }
    DefaultCatalogBusinessFacadeBuilder withWriteNodeLabelApplication(WriteNodeLabelApplication writeNodeLabelApplication) {
        this.writeNodeLabelApplication = writeNodeLabelApplication;
        return this;
    }
    DefaultCatalogBusinessFacadeBuilder withWriteNodePropertiesApplication(WriteNodePropertiesApplication writeNodePropertiesApplication) {
        this.writeNodePropertiesApplication = writeNodePropertiesApplication;
        return this;
    }

    DefaultCatalogBusinessFacadeBuilder withWriteRelationshipPropertiesApplication(WriteRelationshipPropertiesApplication writeRelationshipPropertiesApplication) {
        this.writeRelationshipPropertiesApplication = writeRelationshipPropertiesApplication;
        return this;
    }
    DefaultCatalogBusinessFacadeBuilder withWriteRelationshipsApplication(WriteRelationshipsApplication writeRelationshipsApplication) {
        this.writeRelationshipsApplication = writeRelationshipsApplication;
        return this;
    }

     DefaultCatalogBusinessFacade build(){
            return  new DefaultCatalogBusinessFacade(
                log,
                graphStoreCatalogService,
                projectionMetricsService,
                graphNameValidationService,
                cypherProjectApplication,
                dropGraphApplication,
                dropNodePropertiesApplication,
                dropRelationshipsApplication,
                estimateCommonNeighbourAwareRandomWalkApplication,
                generateGraphApplication,
                graphMemoryUsageApplication,
                graphSamplingApplication,
                listGraphApplication,
                nativeProjectApplication,
                nodeLabelMutatorApplication,
                streamNodePropertiesApplication,
                streamRelationshipPropertiesApplication,
                streamRelationshipsApplication,
                subGraphProjectApplication,
                writeNodeLabelApplication,
                writeNodePropertiesApplication,
                writeRelationshipPropertiesApplication,
                writeRelationshipsApplication
            );
    }
}

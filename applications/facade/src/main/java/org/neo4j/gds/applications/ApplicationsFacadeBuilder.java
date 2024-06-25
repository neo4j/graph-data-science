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
package org.neo4j.gds.applications;

import org.neo4j.gds.applications.graphstorecatalog.CatalogBusinessFacade;

/**
 * This is a helper that makes it easy to inject constituents, and to not have to inject all of them.
 */
public class ApplicationsFacadeBuilder {
    private CatalogBusinessFacade catalogBusinessFacade;
    private CentralityApplications centralityApplications;
    private CommunityApplications communityApplications;
    private NodeEmbeddingApplications nodeEmbeddingApplications;
    private PathFindingApplications pathFindingApplications;
    private SimilarityApplications similarityApplications;

    public ApplicationsFacadeBuilder with(CatalogBusinessFacade catalogBusinessFacade) {
        this.catalogBusinessFacade = catalogBusinessFacade;
        return this;
    }

    public ApplicationsFacadeBuilder with(CentralityApplications centralityApplications) {
        this.centralityApplications = centralityApplications;
        return this;
    }

    public ApplicationsFacadeBuilder with(CommunityApplications communityApplications) {
        this.communityApplications = communityApplications;
        return this;
    }

    public ApplicationsFacadeBuilder with(NodeEmbeddingApplications nodeEmbeddingApplications) {
        this.nodeEmbeddingApplications = nodeEmbeddingApplications;
        return this;
    }

    public ApplicationsFacadeBuilder with(PathFindingApplications pathFindingApplications) {
        this.pathFindingApplications = pathFindingApplications;
        return this;
    }

    public ApplicationsFacadeBuilder with(SimilarityApplications similarityApplications) {
        this.similarityApplications = similarityApplications;
        return this;
    }

    public ApplicationsFacade build() {
        return new ApplicationsFacade(
            catalogBusinessFacade,
            centralityApplications,
            communityApplications,
            nodeEmbeddingApplications,
            pathFindingApplications,
            similarityApplications
        );
    }
}

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
package org.neo4j.gds.procedures.algorithms.community;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.neo4j.gds.procedures.algorithms.community.stats.PushbackCommunityStatsProcedureFacade;
import org.neo4j.gds.procedures.algorithms.community.stream.PushbackCommunityStreamProcedureFacade;

import java.util.Map;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

@ExtendWith(MockitoExtension.class)
class PushbackCommunityProcedureFacadeTest {

    private final String graphName = "g";
    private final Map<String, Object> config = Map.of();

    @Mock
    private PushbackCommunityStreamProcedureFacade streamFacadeMock;

    @Mock
    private PushbackCommunityStatsProcedureFacade statsFacadeMock;

    private PushbackCommunityProcedureFacade facade;

    @BeforeEach
    void setUp() {
        facade = new PushbackCommunityProcedureFacade(
            streamFacadeMock,
            statsFacadeMock
        );
    }
    @Nested
    class Stream {

        @Test
        void approxMaxKCut() {
            facade.approxMaxKCutStream(graphName, config);
            verify(streamFacadeMock).approxMaxKCut(graphName, config);
            verifyNoInteractions(statsFacadeMock);
        }

        @Test
        void cliqueCounting() {
            facade.cliqueCountingStream(graphName, config);
            verify(streamFacadeMock).cliqueCounting(graphName, config);
            verifyNoInteractions(statsFacadeMock);
        }

        @Test
        void conductance() {
            facade.conductanceStream(graphName, config);
            verify(streamFacadeMock).conductance(graphName, config);
            verifyNoInteractions(statsFacadeMock);
        }

        @Test
        void hdbscan() {
            facade.hdbscanStream(graphName, config);
            verify(streamFacadeMock).hdbscan(graphName, config);
            verifyNoInteractions(statsFacadeMock);
        }

        @Test
        void k1Coloring() {
            facade.k1ColoringStream(graphName, config);
            verify(streamFacadeMock).k1Coloring(graphName, config);
            verifyNoInteractions(statsFacadeMock);
        }

        @Test
        void kCore() {
            facade.kCoreStream(graphName, config);
            verify(streamFacadeMock).kCore(graphName, config);
            verifyNoInteractions(statsFacadeMock);
        }

        @Test
        void kMeans() {
            facade.kmeansStream(graphName, config);
            verify(streamFacadeMock).kMeans(graphName, config);
            verifyNoInteractions(statsFacadeMock);
        }

        @Test
        void labelPropagation() {
            facade.labelPropagationStream(graphName, config);
            verify(streamFacadeMock).labelPropagation(graphName, config);
            verifyNoInteractions(statsFacadeMock);
        }

        @Test
        void lcc() {
            facade.localClusteringCoefficientStream(graphName, config);
            verify(streamFacadeMock).lcc(graphName, config);
            verifyNoInteractions(statsFacadeMock);
        }

        @Test
        void leiden() {
            facade.leidenStream(graphName, config);
            verify(streamFacadeMock).leiden(graphName, config);
            verifyNoInteractions(statsFacadeMock);
        }

        @Test
        void louvain() {
            facade.louvainStream(graphName, config);
            verify(streamFacadeMock).louvain(graphName, config);
            verifyNoInteractions(statsFacadeMock);
        }

        @Test
        void modularityOptimization() {
            facade.modularityOptimizationStream(graphName, config);
            verify(streamFacadeMock).modularityOptimization(graphName, config);
            verifyNoInteractions(statsFacadeMock);
        }

        @Test
        void modularity() {
            facade.modularityStream(graphName, config);
            verify(streamFacadeMock).modularity(graphName, config);
            verifyNoInteractions(statsFacadeMock);
        }

        @Test
        void scc() {
            facade.sccStream(graphName, config);
            verify(streamFacadeMock).scc(graphName, config);
            verifyNoInteractions(statsFacadeMock);
        }

        @Test
        void wcc() {
            facade.wccStream(graphName, config);
            verify(streamFacadeMock).wcc(graphName, config);
            verifyNoInteractions(statsFacadeMock);
        }

        @Test
        void triangleCount() {
            facade.triangleCountStream(graphName, config);
            verify(streamFacadeMock).triangleCount(graphName, config);
            verifyNoInteractions(statsFacadeMock);
        }

        @Test
        void trianglesStream() {
            facade.trianglesStream(graphName, config);
            verify(streamFacadeMock).triangles(graphName, config);
            verifyNoInteractions(statsFacadeMock);
        }

        @Test
        void sllpa() {
            facade.sllpaStream(graphName, config);
            verify(streamFacadeMock).sllpa(graphName, config);
            verifyNoInteractions(statsFacadeMock);
        }
    }

    @Nested
    class Stats {


        @Test
        void cliqueCounting() {
            facade.cliqueCountingStats(graphName, config);
            verify(statsFacadeMock).cliqueCounting(graphName, config);
            verifyNoInteractions(streamFacadeMock);
        }

        @Test
        void k1Coloring() {
            facade.k1ColoringStats(graphName, config);
            verify(statsFacadeMock).k1Coloring(graphName, config);
            verifyNoInteractions(streamFacadeMock);
        }

        @Test
        void kCore() {
            facade.kCoreStats(graphName, config);
            verify(statsFacadeMock).kCore(graphName, config);
            verifyNoInteractions(streamFacadeMock);
        }

        @Test
        void kMeans() {
            facade.kmeansStats(graphName, config);
            verify(statsFacadeMock).kMeans(graphName, config);
            verifyNoInteractions(streamFacadeMock);
        }

        @Test
        void labelPropagation() {
            facade.labelPropagationStats(graphName, config);
            verify(statsFacadeMock).labelPropagation(graphName, config);
            verifyNoInteractions(streamFacadeMock);
        }

        @Test
        void leiden() {
            facade.leidenStats(graphName, config);
            verify(statsFacadeMock).leiden(graphName, config);
            verifyNoInteractions(streamFacadeMock);
        }

        @Test
        void louvain() {
            facade.louvainStats(graphName, config);
            verify(statsFacadeMock).louvain(graphName, config);
            verifyNoInteractions(streamFacadeMock);
        }

        @Test
        void modularityOptimization() {
            facade.modularityOptimizationStats(graphName, config);
            verify(statsFacadeMock).modularityOptimization(graphName, config);
            verifyNoInteractions(streamFacadeMock);
        }


        @Test
        void hdbscan() {
            facade.hdbscanStats(graphName, config);
            verify(statsFacadeMock).hdbscan(graphName, config);
            verifyNoInteractions(streamFacadeMock);
        }

        @Test
        void modularity() {
            facade.modularityStats(graphName, config);
            verify(statsFacadeMock).modularity(graphName, config);
            verifyNoInteractions(streamFacadeMock);
        }

        @Test
        void lcc() {
            facade.localClusteringCoefficientStats(graphName, config);
            verify(statsFacadeMock).lcc(graphName, config);
            verifyNoInteractions(streamFacadeMock);
        }

         @Test
        void triangleCount() {
            facade.triangleCountStats(graphName, config);
            verify(statsFacadeMock).triangleCount(graphName, config);
            verifyNoInteractions(streamFacadeMock);
        }


        @Test
        void scc() {
            facade.sccStats(graphName, config);
            verify(statsFacadeMock).scc(graphName, config);
            verifyNoInteractions(streamFacadeMock);
        }

        @Test
        void wcc() {
            facade.wccStats(graphName, config);
            verify(statsFacadeMock).wcc(graphName, config);
            verifyNoInteractions(streamFacadeMock);
        }
        /*
        @Test
        void sllpa() {
            facade.sllpaStream(graphName, config);
            verify(statsFacadeMock).sllpa(graphName, config);
            verifyNoInteractions(streamFacadeMock);
        } */
    }


}

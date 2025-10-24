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
import org.neo4j.gds.procedures.algorithms.community.stream.PushbackCommunityStreamProcedureFacade;

import java.util.Map;

import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class PushbackCommunityProcedureFacadeTest {

    private final String graphName = "g";
    private final Map<String, Object> config = Map.of();

    @Mock
    private PushbackCommunityStreamProcedureFacade streamFacadeMock;

    private PushbackCommunityProcedureFacade facade;

    @BeforeEach
    void setUp() {
        facade = new PushbackCommunityProcedureFacade(
            streamFacadeMock
        );
    }
    @Nested
    class Stream {

        @Test
        void approxMaxKCut() {
            facade.approxMaxKCutStream(graphName, config);
            verify(streamFacadeMock).approxMaxKCut(graphName, config);
            //verifyNoInteractions(mutateFacadeMock, statsFacadeMock, writeFacadeMock);
        }

        @Test
        void cliqueCounting() {
            facade.cliqueCountingStream(graphName, config);
            verify(streamFacadeMock).cliqueCounting(graphName, config);
            //verifyNoInteractions(mutateFacadeMock, statsFacadeMock, writeFacadeMock);
        }

        @Test
        void conductance() {
            facade.conductanceStream(graphName, config);
            verify(streamFacadeMock).conductance(graphName, config);
            //verifyNoInteractions(mutateFacadeMock, statsFacadeMock, writeFacadeMock);
        }

        @Test
        void hdbscan() {
            facade.hdbscanStream(graphName, config);
            verify(streamFacadeMock).hdbscan(graphName, config);
            //verifyNoInteractions(mutateFacadeMock, statsFacadeMock, writeFacadeMock);
        }

        @Test
        void k1Coloring() {
            facade.k1ColoringStream(graphName, config);
            verify(streamFacadeMock).k1Coloring(graphName, config);
            //verifyNoInteractions(mutateFacadeMock, statsFacadeMock, writeFacadeMock);
        }

        @Test
        void kCore() {
            facade.kCoreStream(graphName, config);
            verify(streamFacadeMock).kCore(graphName, config);
            //verifyNoInteractions(mutateFacadeMock, statsFacadeMock, writeFacadeMock);
        }

        @Test
        void kMeans() {
            facade.kmeansStream(graphName, config);
            verify(streamFacadeMock).kMeans(graphName, config);
            //verifyNoInteractions(mutateFacadeMock, statsFacadeMock, writeFacadeMock);
        }

        @Test
        void labelPropagation() {
            facade.labelPropagationStream(graphName, config);
            verify(streamFacadeMock).labelPropagation(graphName, config);
            //verifyNoInteractions(mutateFacadeMock, statsFacadeMock, writeFacadeMock);
        }

        @Test
        void lcc() {
            facade.localClusteringCoefficientStream(graphName, config);
            verify(streamFacadeMock).lcc(graphName, config);
            //verifyNoInteractions(mutateFacadeMock, statsFacadeMock, writeFacadeMock);
        }

        @Test
        void leiden() {
            facade.leidenStream(graphName, config);
            verify(streamFacadeMock).leiden(graphName, config);
            //verifyNoInteractions(mutateFacadeMock, statsFacadeMock, writeFacadeMock);
        }

        @Test
        void louvain() {
            facade.louvainStream(graphName, config);
            verify(streamFacadeMock).louvain(graphName, config);
            //verifyNoInteractions(mutateFacadeMock, statsFacadeMock, writeFacadeMock);
        }

        @Test
        void modularityOptimization() {
            facade.modularityOptimizationStream(graphName, config);
            verify(streamFacadeMock).modularityOptimization(graphName, config);
            //verifyNoInteractions(mutateFacadeMock, statsFacadeMock, writeFacadeMock);
        }

        @Test
        void modularity() {
            facade.modularityStream(graphName, config);
            verify(streamFacadeMock).modularity(graphName, config);
            //verifyNoInteractions(mutateFacadeMock, statsFacadeMock, writeFacadeMock);
        }

        @Test
        void scc() {
            facade.sccStream(graphName, config);
            verify(streamFacadeMock).scc(graphName, config);
            //verifyNoInteractions(mutateFacadeMock, statsFacadeMock, writeFacadeMock);
        }

        @Test
        void wcc() {
            facade.wccStream(graphName, config);
            verify(streamFacadeMock).wcc(graphName, config);
            //verifyNoInteractions(mutateFacadeMock, statsFacadeMock, writeFacadeMock);
        }
    }

}

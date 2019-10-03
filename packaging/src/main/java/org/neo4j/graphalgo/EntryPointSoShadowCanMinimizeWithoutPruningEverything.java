/*
 * Copyright (c) 2017-2019 "Neo4j,"
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

package org.neo4j.graphalgo;

import org.neo4j.graphalgo.linkprediction.LinkPredictionFunc;
import org.neo4j.graphalgo.similarity.SimilaritiesFunc;
import org.neo4j.graphalgo.unionfind.MSColoringProc;
import org.neo4j.graphalgo.unionfind.UnionFindProc;
import org.neo4j.graphalgo.wcc.WccProc;
// Don't remove these imports
import org.neo4j.procedure.Procedure;
import org.neo4j.procedure.UserFunction;

public class EntryPointSoShadowCanMinimizeWithoutPruningEverything {

    static {
        Class[] labsProcs = new Class[]{
                AllShortestPathsProc.class,
                ApproxNearestNeighborsProc.class,
                BalancedTriadsProc.class,
                BetweennessCentralityProc.class,
                ClosenessCentralityProc.class,
                CosineProc.class,
                DangalchevCentralityProc.class,
                DegreeCentralityProc.class,
                EuclideanProc.class,
                HarmonicCentralityProc.class,
                InfoMapProc.class,
                JaccardProc.class,
                KShortestPathsProc.class,
                KSpanningTreeProc.class,
                NodeWalkerProc.class,
                OverlapProc.class,
                PearsonProc.class,
                PrimProc.class,
                ShortestPathDeltaSteppingProc.class,
                ShortestPathProc.class,
                ShortestPathsProc.class,
                SimilarityProc.class,
                StronglyConnectedComponentsProc.class,
                TraverseProc.class,
                TriangleProc.class,
                UtilityProc.class,
                MSColoringProc.class
        };
        Class[] labsFuncs = new Class[]{
                IsFiniteFunc.class,
                LinkPredictionFunc.class,
                SimilaritiesFunc.class,
                OneHotEncodingFunc.class,
        };
        Class[] productProcs = new Class[]{
                ArticleRankProc.class,
                EigenvectorCentralityProc.class,
                GraphLoadProc.class,
                LabelPropagationProc.class,
                ListProc.class,
                LouvainProc.class,
                MemRecProc.class,
                PageRankProc.class,
                WccProc.class,
                UnionFindProc.class
        };
        Class[] productFuncs = new Class[]{
                GetNodeFunc.class,
                VersionFunc.class
        };
    }
}
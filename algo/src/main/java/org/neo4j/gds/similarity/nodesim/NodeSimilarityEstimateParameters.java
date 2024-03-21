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
package org.neo4j.gds.similarity.nodesim;

import org.neo4j.gds.annotation.Parameters;

@Parameters
public final class NodeSimilarityEstimateParameters {

    public static NodeSimilarityEstimateParameters create(
        int normalizedK,
        int normalizedN,
        boolean useComponents,
        boolean runWCC,
        boolean computeToGraph
    ) {
        return new NodeSimilarityEstimateParameters(
            normalizedK,
            normalizedN,
            useComponents, runWCC, computeToGraph
        );
    }


    private final int normalizedK;
    private final int normalizedN;
    private final boolean computeToGraph;
    private final boolean useComponents;
    private final boolean runWCC;

    private NodeSimilarityEstimateParameters(
        int normalizedK,
        int normalizedN,
        boolean useComponents,
        boolean runWCC,
        boolean computeToGraph
    ) {

        this.normalizedK = normalizedK;
        this.normalizedN = normalizedN;
        this.computeToGraph = computeToGraph;
        this.useComponents = useComponents;
        this.runWCC = runWCC;
    }


    public int normalizedK() {
        return normalizedK;
    }

    public int normalizedN() {
        return normalizedN;
    }

    public boolean computeToGraph() {return computeToGraph;}

    boolean hasTopK() {
        return normalizedK != 0;
    }

    boolean hasTopN() {
        return normalizedN != 0;
    }

    // WCC specialization

    public boolean useComponents() {
        return useComponents;
    }



    boolean runWCC() {
        return runWCC;
    }


}

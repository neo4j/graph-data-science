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
package org.neo4j.gds.ml.kge;

import com.carrotsearch.hppc.DoubleArrayList;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.nodeproperties.DoubleArrayTestPropertyValues;
import org.neo4j.gds.nodeproperties.FloatArrayTestPropertyValues;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.gds.ml.kge.ScoreFunction.DISTMULT;
import static org.neo4j.gds.ml.kge.ScoreFunction.TRANSE;

public class LinkScorerFactoryTest {

    @Test
    void linkScorerFactoryCreateCorrectScorer() {
        var transe = LinkScorerFactory.create(TRANSE, new FloatArrayTestPropertyValues(l -> new float[]{0}),
            DoubleArrayList.from(0)
        );
        assertThat(transe).isInstanceOf(FloatEuclideanDistanceLinkScorer.class);

        var distmult = LinkScorerFactory.create(DISTMULT, new FloatArrayTestPropertyValues(l -> new float[]{0}),
            DoubleArrayList.from(0)
        );
        assertThat(distmult).isInstanceOf(FloatDistMultLinkScorer.class);

        var transeDouble = LinkScorerFactory.create(TRANSE, new DoubleArrayTestPropertyValues(l -> new double[]{0}),
            DoubleArrayList.from(0)
        );
        assertThat(transeDouble).isInstanceOf(DoubleEuclideanDistanceLinkScorer.class);

        var distmultDouble = LinkScorerFactory.create(DISTMULT, new DoubleArrayTestPropertyValues(l -> new double[]{0}),
            DoubleArrayList.from(0)
        );
        assertThat(distmultDouble).isInstanceOf(DoubleDistMultLinkScorer.class);
    }
}

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
package org.neo4j.gds.sllpa;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.utils.CloseableThreadLocal;

import java.lang.invoke.MethodHandles;
import java.util.Optional;
import java.util.SplittableRandom;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;

class SpeakerListenerLPAComputationTest {

    @Test
    void closesThreadLocal() {

        var computation = new SpeakerListenerLPAComputation(Optional.empty());
        computation.close();

        CloseableThreadLocal<SplittableRandom> threadLocal = null;
        try {
            //noinspection unchecked
            threadLocal = (CloseableThreadLocal<SplittableRandom>) MethodHandles
                .privateLookupIn(SpeakerListenerLPAComputation.class, MethodHandles.lookup())
                .findGetter(SpeakerListenerLPAComputation.class, "random", CloseableThreadLocal.class)
                .invoke(computation);
        } catch (Throwable e) {
            fail("couldn't inspect the field", e);
        }
        assertThatThrownBy(threadLocal::get).isInstanceOf(NullPointerException.class);
    }

}

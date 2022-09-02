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
package org.neo4j.gds.pregel;

import org.apache.commons.lang3.reflect.FieldUtils;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.beta.pregel.Pregel;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.IdFunction;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.TestGraph;
import org.neo4j.gds.utils.CloseableThreadLocal;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;
import static org.neo4j.gds.pregel.SpeakerListenerLPA.LABELS_PROPERTY;

@GdlExtension
class SpeakerListenerLPATest {

    @GdlGraph
    private static final String GDL =
        "(x), (a), (b), (c), (d), (e), (f), (g), (h), (i)" +
        ", (a)-->(b)" +
        ", (a)-->(c)" +
        ", (b)-->(e)" +
        ", (b)-->(d)" +
        ", (b)-->(c)" +
        ", (e)-->(f)" +
        ", (f)-->(g)" +
        ", (f)-->(h)" +
        ", (f)-->(i)" +
        ", (h)-->(i)" +
        ", (g)-->(i)";

    @Inject
    private TestGraph graph;

    @Inject
    private IdFunction idFunction;

    @Test
    void testWithoutPruning() {
        var config = ImmutableSpeakerListenerLPAConfig.builder().concurrency(1).minAssociationStrength(0.00).maxIterations(10).build();

        var pregelJob = Pregel.create(
            graph,
            config,
            new SpeakerListenerLPA(42),
            Pools.DEFAULT,
            ProgressTracker.NULL_TRACKER
        );

        var resultCommunities = pregelJob.run().nodeValues().longArrayProperties(LABELS_PROPERTY);

        Map<Long, Set<Long>> communities = new HashMap<>();

        graph.forEachNode(nodeId -> {
            for (long communityId : resultCommunities.get(nodeId)) {
               communities.compute(communityId, (ignore, members) -> {
                   if (members == null) {
                       members = new HashSet<>();
                   }

                   members.add(nodeId);
                   return members;
               });
            }

            return true;
        });

        var expected = Map.of(
            0L, Set.of(0L),
            1L, Set.of(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L),
            2L, Set.of(2L, 4L, 5L, 6L, 7L, 8L, 9L),
            3L, Set.of(3L),
            4L, Set.of(4L),
            5L, Set.of(5L, 6L, 7L, 8L, 9L),
            6L, Set.of(6L, 7L, 8L, 9L),
            7L, Set.of(7L),
            8L, Set.of(8L),
            9L, Set.of(9L)
        );

        assertThat(communities).containsExactlyInAnyOrderEntriesOf(expected);
    }

    @Test
    void prunesAwayAfterManyIterations() {
        var config = ImmutableSpeakerListenerLPAConfig.builder().concurrency(1).maxIterations(30).build();

        var pregelJob = Pregel.create(
            graph,
            config,
            new SpeakerListenerLPA(42),
            Pools.DEFAULT,
            ProgressTracker.NULL_TRACKER
        );

        var resultCommunities = pregelJob.run().nodeValues().longArrayProperties(LABELS_PROPERTY);

        Map<Long, Set<Long>> communities = new HashMap<>();

        graph.forEachNode(nodeId -> {
            for (long communityId : resultCommunities.get(nodeId)) {
                communities.compute(communityId, (ignore, members) -> {
                    if (members == null) {
                        members = new HashSet<>();
                    }

                    members.add(nodeId);
                    return members;
                });
            }

            return true;
        });

        var expected = Map.of(
            0L, Set.of(0L),
            1L, Set.of(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L),
            2L, Set.of(8L, 9L)
        );

        assertThat(communities).containsExactlyInAnyOrderEntriesOf(expected);
    }

    @Test
    void closesThreadLocal() {
        var config = SpeakerListenerLPAConfigImpl.builder().concurrency(1).maxIterations(30).build();

        var computation = new SpeakerListenerLPA(42);
        Pregel.create(
            graph,
            config,
            computation,
            Pools.DEFAULT,
            ProgressTracker.NULL_TRACKER
        ).run();

        Field threadLocalField = FieldUtils.getField(SpeakerListenerLPA.class, "random", true);
        try {
            //noinspection unchecked
            final var threadLocal = (CloseableThreadLocal<Random>) threadLocalField.get(computation);
            assertThatThrownBy(threadLocal::get).isInstanceOf(NullPointerException.class);
        } catch (IllegalAccessException e) {
            fail("couldn't inspect the field");
        }
    }

}

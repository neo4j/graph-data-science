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
package org.neo4j.graphalgo.junit.annotation;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.core.GdsEdition;

import static org.assertj.core.api.Assertions.assertThat;

class GdsEnterpriseEditionExtensionTest {

    @Test
    void shouldSetGdsEditionsCorrectly() {
        var extension = new GdsEnterpriseEditionExtension();

        assertThat(GdsEdition.instance().isOnCommunityEdition()).isTrue();

        // BeforeEach
        extension.beforeEach(null);
        assertThat(GdsEdition.instance().isOnEnterpriseEdition()).isTrue();

        // AfterEach
        extension.afterEach(null);
        assertThat(GdsEdition.instance().isOnCommunityEdition()).isTrue();

        // BeforeTestExecution
        extension.beforeTestExecution(null);
        assertThat(GdsEdition.instance().isOnEnterpriseEdition()).isTrue();

        // AfterTestExecution
        extension.afterTestExecution(null);
        assertThat(GdsEdition.instance().isOnCommunityEdition()).isTrue();
    }

    @Nested
    @GdsEnterpriseEdition
    class BeforeEachTest {

        @Test
        void shouldRunWithGdsEnterpriseEdition() {
            assertThat(GdsEdition.instance().isOnEnterpriseEdition()).isTrue();
        }

        @Test
        void shouldStillRunWithGdsEnterpriseEdition() {
            assertThat(GdsEdition.instance().isOnEnterpriseEdition()).isTrue();
        }
    }

    @Nested
    class BeforeTestExecutionTest {

        @Test
        @GdsEnterpriseEdition
        void shouldRunWithGdsEnterpriseEdition() {
            assertThat(GdsEdition.instance().isOnEnterpriseEdition()).isTrue();
        }

        @Test
        void shouldRunWithGdsCommunityEdition() {
            assertThat(GdsEdition.instance().isOnCommunityEdition()).isTrue();
        }
    }
}

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
package org.neo4j.gds.api;

import org.junit.jupiter.api.Test;

import java.util.HashSet;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

class UserTest {
    @Test
    void shouldParseUsernameOverride() {
        assertThat(User.parseUsernameOverride("a proper username")).hasValue("a proper username");
        assertThat(User.parseUsernameOverride("   a padded username   ")).hasValue("a padded username");

        assertThat(User.parseUsernameOverride(null)).isEmpty();
        assertThat(User.parseUsernameOverride("")).isEmpty();
        assertThat(User.parseUsernameOverride("   ")).isEmpty();
    }

    @Test
    void shouldDoEqualityOnNameOnly() {
        var user1 = new User("a", false);
        var user2 = new User("a", true);
        var user3 = new User("b", false);

        assertThat(user1).isEqualTo(user1);
        assertThat(user1).isEqualTo(user2);
        assertThat(user1).isNotEqualTo(user3);
    }

    @Test
    void shouldDoHashcodeOnNameOnly() {
        var users = new HashSet<>();

        var user1 = new User("a", false);
        var user2 = new User("a", true);
        var user3 = new User("b", false);

        assertThat(users).hasSize(0);

        users.add(user1);
        assertThat(users).hasSize(1);
        assertTrue(users.contains(user1));
        assertTrue(users.contains(user2));

        users.clear();

        users.add(user2);
        assertThat(users).hasSize(1);
        assertTrue(users.contains(user1));
        assertTrue(users.contains(user2));

        users.add(user3);
        assertThat(users).hasSize(2);
        assertTrue(users.contains(user1));
        assertTrue(users.contains(user2));
        assertTrue(users.contains(user3));
    }
}

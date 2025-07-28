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
package org.neo4j.gds.procedures;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.api.User;
import org.neo4j.internal.kernel.api.security.AuthSubject;
import org.neo4j.internal.kernel.api.security.SecurityContext;

import java.util.Set;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class UserAccessorTest {

    public static Stream<Arguments> roleAdminProviderAuraDS() {
        return Stream.of(
            Arguments.of("admin", true),
            Arguments.of("console_admin_vdc", true),
            Arguments.of("console_admin_pro_123445", true),
            Arguments.of("user", false),
            Arguments.of("console_user", false)
        );
    }

    public static Stream<Arguments> roleAdminProviderPlugin() {
        return Stream.of(
            Arguments.of("admin", true),
            Arguments.of("console_admin", false),
            Arguments.of("console_admin_pro_123445", false),
            Arguments.of("user", false),
            Arguments.of("console_user", false)
        );
    }

    @ParameterizedTest
    @MethodSource("roleAdminProviderAuraDS")
    void testAuraAdminRoles(String role, boolean expected) {
        var userAccessor = UserAccessor.createForAuraDS();

        var securityContext = mock(SecurityContext.class);
        var mockSubject = mock(AuthSubject.class);
        when(securityContext.subject()).thenReturn(mockSubject);
        when(securityContext.roles()).thenReturn(Set.of("foo", role, "bar"));
        when(mockSubject.executingUser()).thenReturn("foo");

        User user = userAccessor.getUser(securityContext);

        assertThat(user.isAdmin()).isEqualTo(expected);
    }

    @ParameterizedTest
    @MethodSource("roleAdminProviderPlugin")
    void testAdminRoles(String role, boolean expected) {
        var userAccessor = UserAccessor.create();

        var securityContext = mock(SecurityContext.class);
        var mockSubject = mock(AuthSubject.class);
        when(securityContext.subject()).thenReturn(mockSubject);
        when(securityContext.roles()).thenReturn(Set.of("foo", role, "bar"));
        when(mockSubject.executingUser()).thenReturn("foo");

        User user = userAccessor.getUser(securityContext);

        assertThat(user.isAdmin()).isEqualTo(expected);
    }

}

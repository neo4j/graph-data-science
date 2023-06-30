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
package org.neo4j.gds;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class SysInfoProcFacadeTest {

    @Test
    void shouldTransformLicenseDetailsToProcedureResult() {
        var licensingBusinessFacadeMock = mock(LicensingBusinessFacade.class);
        // This is a bit dodgy, can we come up with a better way to get the Details 🤔
        var licenseDetailsMock = mock(LicenseDetails.class);
        when(licenseDetailsMock.isLicensed()).thenReturn(true);
        when(licenseDetailsMock.details()).thenReturn("XYZ");
        when(licensingBusinessFacadeMock.licenseDetails()).thenReturn(licenseDetailsMock);

        var sysInfoProcFacade = new SysInfoProcFacade(licensingBusinessFacadeMock);

        var licenseStateResult = sysInfoProcFacade.licenseStateResult();

        assertThat(licenseStateResult.isLicensed).isTrue();
        assertThat(licenseStateResult.details).isEqualTo("XYZ");
    }

}

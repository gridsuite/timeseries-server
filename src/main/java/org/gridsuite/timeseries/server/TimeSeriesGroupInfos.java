/**
 * Copyright (c) 2022, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package org.gridsuite.timeseries.server;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import java.util.UUID;

//More infos can be added later?
@AllArgsConstructor
@Getter
@Setter
public class TimeSeriesGroupInfos {

    public UUID id;

    public static TimeSeriesGroupInfos fromEntity(TimeSeriesGroupEntity tsg) {
        return new TimeSeriesGroupInfos(tsg.getId());
    }
}

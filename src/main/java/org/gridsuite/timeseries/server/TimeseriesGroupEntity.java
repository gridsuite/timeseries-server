/**
 * Copyright (c) 2022, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package org.gridsuite.timeseries.server;

import java.util.UUID;

import javax.persistence.*;
import lombok.*;
import nonapi.io.github.classgraph.json.Id;


/**
 * @author Jon Schuhmacher <jon.harper at rte-france.com>
 */

@NoArgsConstructor
@AllArgsConstructor
@Getter
@Setter
@Entity
@Builder
@Table(name = "timeseries")
public class TimeseriesGroupEntity {

    @Id
    @Column(name = "id")
    private UUID id;

    //Maybe add metadata here about this group ?
    // - name ?
    // - number of timeseries ?
    // - tags (countries, periods, kind ...?)
    // - something else ?

    //TODO store here powsybl's timeseries.getMetadata() ?

}


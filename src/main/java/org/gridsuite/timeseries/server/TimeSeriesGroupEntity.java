/**
 * Copyright (c) 2022, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package org.gridsuite.timeseries.server;

import java.util.UUID;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * @author Jon Schuhmacher <jon.harper at rte-france.com>
 */
@NoArgsConstructor // for hibernate
@AllArgsConstructor
@Getter
@Setter
@Entity
@Table(name = "timeseries_group")
public class TimeSeriesGroupEntity {

    public TimeSeriesGroupEntity(String indexType, String index, String metadatas) {
        this(null, indexType, index, metadatas);
    }

    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    @Column(name = "id")
    private UUID id;

    @Column(name = "index_type")
    // TODO proper modeling of this data instead of json string
    private String indexType;

    @Column(name = "index", columnDefinition = "CLOB")
    // TODO proper modeling of this data instead of json string
    private String index;

    @Column(name = "metadatas", columnDefinition = "CLOB")
    // TODO proper modeling of this data instead of json string
    private String metadatas;

    // TODO Maybe add metadata here about this group ?
    // - name ?
    // - number of timeSeries ?
    // - tags (countries, periods, kind ...?)
    // - something else ?
    // different from each timeseries metadatas

}


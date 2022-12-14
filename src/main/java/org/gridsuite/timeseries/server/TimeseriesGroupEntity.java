/**
 * Copyright (c) 2022, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package org.gridsuite.timeseries.server;

import java.util.UUID;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;

import lombok.Getter;
import lombok.Setter;


/**
 * @author Jon Schuhmacher <jon.harper at rte-france.com>
 */

@Getter
@Setter
@Entity
@Table(name = "timeseries_group")
public class TimeseriesGroupEntity {

    public TimeseriesGroupEntity() {
        this.indexType = null;
        this.index = null;
        this.metadatas = null;
    }

    public TimeseriesGroupEntity(String indexType, String index, String metadatas) {
        this.indexType = indexType;
        this.index = index;
        this.metadatas = metadatas;
    }

    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    @Column(name = "id")
    private UUID id;

    @Column(name = "index_type")
    // TODO proper modeling of this data instead of json string
    private final String indexType;

    @Column(name = "index", columnDefinition = "CLOB")
    // TODO proper modeling of this data instead of json string
    private final String index;

    @Column(name = "metadatas", columnDefinition = "CLOB")
    // TODO proper modeling of this data instead of json string
    private final String metadatas;

    // TODO Maybe add metadata here about this group ?
    // - name ?
    // - number of timeseries ?
    // - tags (countries, periods, kind ...?)
    // - something else ?
    // different from each timeseries metadatas

}


/**
 * Copyright (c) 2022, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package org.gridsuite.study.server.service;

import org.springframework.stereotype.Service;

/**
 * @author Jon Schuhmacher <jon.harper at rte-france.com>
 */
@Service
public class TimeseriesService {

    private final TimeseriesRepository timeseriesRepository;
    private final TimeseriesDataRepository timeseriesDataRepository;

    @Transactional
    public UUID createTimeseriesGroup(List<Timeseries> timeseries) {
        TimeseriesGroupEntity tsGroup = TimeseriesRepository.save(new TimeseriesGroupEntity());
        timeseriesDataRepository.save(uuid, timeseries);
        return tsGroup.getId();
    }

    @Transactional
    public void updateTimeseriesGroup(UUID uuid, List<Timeseries> timeseries) {
        TimeseriesGroupEntity tsGroup = TimeseriesRepository.findById(uuid);
        timeseriesDataRepository.save(uuid, timeseries);
    }

    @Transactional
    public void getTimeseriesGroup(UUID uuid, List<Timeseries> timeseries) {
        List<Timeseries> tsData = TimeseriesDataRepository.findById(uuid);
        return tsData;
    }

}

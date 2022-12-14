/**
 * Copyright (c) 2022, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package org.gridsuite.timeseries.server;

import java.util.List;
import java.util.UUID;

import javax.transaction.Transactional;

import org.springframework.stereotype.Service;

import com.powsybl.timeseries.TimeSeries;

/**
 * @author Jon Schuhmacher <jon.harper at rte-france.com>
 */
@Service
public class TimeseriesService {

    private final TimeseriesGroupRepository timeseriesGroupRepository;
    private final TimeseriesDataRepository timeseriesDataRepository;

    public List<TimeseriesGroupEntity> getTimeseriesGroupsList() {
        return timeseriesGroupRepository.findAll();
    }

    public TimeseriesService(TimeseriesGroupRepository timeseriesGroupRepository,
            TimeseriesDataRepository timeseriesDataRepository) {
        this.timeseriesGroupRepository = timeseriesGroupRepository;
        this.timeseriesDataRepository = timeseriesDataRepository;
    }

    @Transactional
    public UUID createTimeseriesGroup(List<TimeSeries> timeseries) {
        // TODO assert index is the same
        // TODO save index
        TimeseriesGroupEntity tsGroup = timeseriesGroupRepository.save(new TimeseriesGroupEntity());
        timeseriesDataRepository.save(tsGroup.getId(), timeseries);
        return tsGroup.getId();
    }

    @Transactional
    public List<TimeSeries> getTimeseriesGroup(UUID uuid, boolean tryToCompress, String time, String col) {
        // TODO return saved index
        List<TimeSeries> tsData = timeseriesDataRepository.findById(uuid, tryToCompress, time, col);
        return tsData;
    }

    @Transactional
    public void deleteTimeseriesGroup(UUID uuid) {
        timeseriesDataRepository.delete(uuid);
        timeseriesGroupRepository.deleteById(uuid);
    }

}

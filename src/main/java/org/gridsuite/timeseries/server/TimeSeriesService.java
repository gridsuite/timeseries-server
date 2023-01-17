/**
 * Copyright (c) 2022, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package org.gridsuite.timeseries.server;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.transaction.Transactional;

import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.powsybl.timeseries.TimeSeries;
import com.powsybl.timeseries.TimeSeriesIndex;
import com.powsybl.timeseries.TimeSeriesMetadata;

/**
 * @author Jon Schuhmacher <jon.harper at rte-france.com>
 */
@Service
public class TimeSeriesService {

    private final TimeSeriesGroupRepository timeseriesGroupRepository;
    private final TimeSeriesDataRepository timeseriesDataRepository;
    private final TimeSeriesMetadataService timeseriesMetadataService;

    // TODO to remove when metadata are properly modeled
    private final ObjectMapper objectmapper;

    public List<Map<String, UUID>> getTimeseriesGroupsIds() {
        return timeseriesGroupRepository.findAll().stream()
                .map(tsGroup -> Map.of("id", tsGroup.getId()))
                .collect(Collectors.toList());
    }

    public TimeSeriesService(TimeSeriesGroupRepository timeseriesGroupRepository,
            TimeSeriesDataRepository timeseriesDataRepository, TimeSeriesMetadataService timeseriesMetadataService,
            ObjectMapper objectMapper) {
        this.timeseriesGroupRepository = timeseriesGroupRepository;
        this.timeseriesDataRepository = timeseriesDataRepository;
        this.timeseriesMetadataService = timeseriesMetadataService;
        this.objectmapper = objectMapper;
    }

    private void synchronizeIndex(List<TimeSeries> timeseries) {
        // For now this just throws when the index is not the same for all
        TimeSeriesIndex index = timeseries.get(0).getMetadata().getIndex();
        for (TimeSeries ts : timeseries) {
            try {
                ts.synchronize(index);
            } catch (UnsupportedOperationException e) {
                //TODO better messages (return all problems at once?)
                //TODO use better API than catching UnsupportedOperationException
                //TODO better separation of service API and controller API: don't speak http here
                throw new ResponseStatusException(HttpStatus.BAD_REQUEST,
                        "Different index for " + timeseries.get(0).getMetadata().getName()
                        + "and " + ts.getMetadata().getName());
            }
        }
    }

    @Transactional
    public UUID createTimeseriesGroup(List<TimeSeries> timeseries) {
        synchronizeIndex(timeseries);

        // TODO proper modeling instead of json
        TimeSeriesIndex index = timeseries.get(0).getMetadata().getIndex();
        String indexType = index.getType();
        String indexJson = timeseriesMetadataService.indexToJson(index);
        String metadatasJson = timeseriesMetadataService.individualTimeSeriesMetadatasToJson(timeseries);

        TimeSeriesGroupEntity tsGroup = timeseriesGroupRepository.save(new TimeSeriesGroupEntity(indexType, indexJson, metadatasJson));
        timeseriesDataRepository.save(tsGroup.getId(), timeseries);
        return tsGroup.getId();
    }

    @Transactional
    public String getTimeseriesGroupMetadataJson(UUID uuid) {
        TimeSeriesGroupEntity timeseriesGroupEntity = timeseriesGroupRepository.findById(uuid).orElseThrow();
        TimeSeriesIndex index = timeseriesMetadataService.indexFromJson(timeseriesGroupEntity.getIndexType(), timeseriesGroupEntity.getIndex());
        List<TimeSeriesMetadata> metadatas = timeseriesMetadataService.timeSeriesMetadataListFromJson(index, timeseriesGroupEntity.getMetadatas());
        return timeseriesMetadataService.allMetadatasToJson(timeseriesGroupEntity.getId(), index, metadatas);
    }

    @Transactional
    public List<TimeSeries> getTimeseriesGroup(UUID uuid, boolean tryToCompress, String time, List<String> timeSeriesNames) {
        TimeSeriesGroupEntity tsGroup = timeseriesGroupRepository.findById(uuid).orElseThrow();
        TimeSeriesIndex index = timeseriesMetadataService.indexFromJson(tsGroup.getIndexType(), tsGroup.getIndex());
        Map<String, Object> individualMetadatas = timeseriesMetadataService
                .individualMetadatasMapFromJson(tsGroup.getMetadatas());

        List<TimeSeries> tsData = timeseriesDataRepository.findById(index, individualMetadatas, tsGroup.getId(), tryToCompress, time, timeSeriesNames);
        Map<String, TimeSeries> tsDataByName = tsData.stream().collect(Collectors.toMap(ts -> ts.getMetadata().getName(), Function.identity()));
        List<TimeSeries> tsDataOrdered = individualMetadatas.keySet().stream().flatMap(
            name -> Optional.ofNullable(tsDataByName.get(name)).stream()
        ).collect(Collectors.toList());
        return tsDataOrdered;
    }

    @Transactional
    public void deleteTimeseriesGroup(UUID uuid) {
        timeseriesDataRepository.delete(uuid);
        timeseriesGroupRepository.deleteById(uuid);
    }

}

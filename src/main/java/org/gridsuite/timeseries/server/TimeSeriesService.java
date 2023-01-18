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

    private final TimeSeriesGroupRepository timeSeriesGroupRepository;
    private final TimeSeriesDataRepository timeSeriesDataRepository;
    private final TimeSeriesMetadataService timeSeriesMetadataService;

    // TODO to remove when metadata are properly modeled
    private final ObjectMapper objectmapper;

    public List<TimeSeriesGroupInfos> getAllTimeSeriesGroupsInfos() {
        return timeSeriesGroupRepository.findAll().stream()
                .map(tsGroup -> TimeSeriesGroupInfos.fromEntity(tsGroup))
                .collect(Collectors.toList());
    }

    public TimeSeriesService(TimeSeriesGroupRepository timeSeriesGroupRepository,
            TimeSeriesDataRepository timeSeriesDataRepository, TimeSeriesMetadataService timeSeriesMetadataService,
            ObjectMapper objectMapper) {
        this.timeSeriesGroupRepository = timeSeriesGroupRepository;
        this.timeSeriesDataRepository = timeSeriesDataRepository;
        this.timeSeriesMetadataService = timeSeriesMetadataService;
        this.objectmapper = objectMapper;
    }

    private void synchronizeIndex(List<TimeSeries> timeSeries) {
        // For now this just throws when the index is not the same for all
        TimeSeriesIndex index = timeSeries.get(0).getMetadata().getIndex();
        for (TimeSeries ts : timeSeries) {
            try {
                ts.synchronize(index);
            } catch (UnsupportedOperationException e) {
                //TODO better messages (return all problems at once?)
                //TODO use better API than catching UnsupportedOperationException
                //TODO better separation of service API and controller API: don't speak http here
                throw new ResponseStatusException(HttpStatus.BAD_REQUEST,
                        "Different index for " + timeSeries.get(0).getMetadata().getName()
                        + "and " + ts.getMetadata().getName());
            }
        }
    }

    @Transactional
    public TimeSeriesGroupInfos createTimeSeriesGroup(List<TimeSeries> timeSeries) {
        synchronizeIndex(timeSeries);

        // TODO proper modeling instead of json
        TimeSeriesIndex index = timeSeries.get(0).getMetadata().getIndex();
        String indexType = index.getType();
        String indexJson = timeSeriesMetadataService.indexToJson(index);
        String metadatasJson = timeSeriesMetadataService.individualTimeSeriesMetadatasToJson(timeSeries);

        TimeSeriesGroupEntity tsGroup = timeSeriesGroupRepository.save(new TimeSeriesGroupEntity(indexType, indexJson, metadatasJson));
        timeSeriesDataRepository.save(tsGroup.getId(), timeSeries);
        return TimeSeriesGroupInfos.fromEntity(tsGroup);
    }

    @Transactional
    public String getTimeSeriesGroupMetadataJson(UUID uuid) {
        TimeSeriesGroupEntity timeSeriesGroupEntity = timeSeriesGroupRepository.findById(uuid).orElseThrow();
        TimeSeriesIndex index = timeSeriesMetadataService.indexFromJson(timeSeriesGroupEntity.getIndexType(), timeSeriesGroupEntity.getIndex());
        List<TimeSeriesMetadata> metadatas = timeSeriesMetadataService.timeSeriesMetadataListFromJson(index, timeSeriesGroupEntity.getMetadatas());
        return timeSeriesMetadataService.allMetadatasToJson(timeSeriesGroupEntity.getId(), index, metadatas);
    }

    @Transactional
    public List<TimeSeries> getTimeSeriesGroup(UUID uuid, boolean tryToCompress, String time, List<String> timeSeriesNames) {
        TimeSeriesGroupEntity tsGroup = timeSeriesGroupRepository.findById(uuid).orElseThrow();
        TimeSeriesIndex index = timeSeriesMetadataService.indexFromJson(tsGroup.getIndexType(), tsGroup.getIndex());
        Map<String, Object> individualMetadatas = timeSeriesMetadataService
                .individualMetadatasMapFromJson(tsGroup.getMetadatas());

        List<TimeSeries> tsData = timeSeriesDataRepository.findById(index, individualMetadatas, tsGroup.getId(), tryToCompress, time, timeSeriesNames);
        Map<String, TimeSeries> tsDataByName = tsData.stream().collect(Collectors.toMap(ts -> ts.getMetadata().getName(), Function.identity()));
        List<TimeSeries> tsDataOrdered = individualMetadatas.keySet().stream().flatMap(
            name -> Optional.ofNullable(tsDataByName.get(name)).stream()
        ).collect(Collectors.toList());
        return tsDataOrdered;
    }

    @Transactional
    public void deleteTimeSeriesGroup(UUID uuid) {
        timeSeriesDataRepository.delete(uuid);
        timeSeriesGroupRepository.deleteById(uuid);
    }

}

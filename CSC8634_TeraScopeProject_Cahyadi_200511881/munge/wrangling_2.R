library(ProjectTemplate)
library(dplyr)
library(lubridate)
library(ggplot2)
load.project()

# This second file for wrangling the data is to tackle the problem found on the 
# first wrangling in where for the event name "Saving Config", "Tiling", and
# "Uploading" some of their GPU condition could not be monitored since the
# GPU data only take note of the GPU condition every 2s. Therefore we gathered the
# GPU condition for some of the individuals that have missing values by using 
# the interpolation between the two closest GPU conditions that happen right before
# and right after these event names. 

appCheckpointRawDistinct = unique(Copy.of.application.checkpoints)

gpuRawDistinct = unique(Copy.of.gpu)



appDataframe = appCheckpointRawDistinct
GPUdataframe = gpuRawDistinct

hostnameListF = unique(appDataframe$hostname)
finalData_2 = data.frame()

for (hostf in hostnameListF){
  
  tempHostnameAppFunction = data.frame()
  tempHostnameGPUFunction = data.frame()
  tempTaskIdFunction = vector()
  tempFinishedData = data.frame()
  
  tempHostnameAppFunction = subset(appDataframe, appDataframe$hostname == hostf)
  tempHostnameAppFunction[,"timestamp_H_M_S"] = hms(substr(tempHostnameAppFunction$timestamp,12,23))
  tempHostnameGPUFunction = subset(GPUdataframe, GPUdataframe$hostname == hostf)
  tempHostnameGPUFunction[,"timestamp_H_M_S"] = hms(substr(tempHostnameGPUFunction$timestamp,12,23))
  
  # Process the Application Data first
  
  # Total Render Event Name
  # Separate the Start and Stop event type into two data frame
  startAppDataTotalRender = subset(tempHostnameAppFunction, 
                                   (tempHostnameAppFunction$eventName == "TotalRender" &
                                      tempHostnameAppFunction$eventType == "START"))
  stopAppDataTotalRender = subset(tempHostnameAppFunction,
                                  (tempHostnameAppFunction$eventName == "TotalRender" &
                                     tempHostnameAppFunction$eventType == "STOP"))
  
  # Saving Config Event Name
  
  startAppDataSavingConfig = subset(tempHostnameAppFunction,
                                    (tempHostnameAppFunction$eventName == "Saving Config" &
                                       tempHostnameAppFunction$eventType == "START"))
  
  stopAppDataSavingConfig = subset(tempHostnameAppFunction,
                                   (tempHostnameAppFunction$eventName == "Saving Config" &
                                      tempHostnameAppFunction$eventType == "STOP"))
  
  #Render Event name
  
  startAppDataRender = subset(tempHostnameAppFunction,
                              (tempHostnameAppFunction$eventName == "Render" &
                                 tempHostnameAppFunction$eventType == "START"))
  stopAppDataRender = subset(tempHostnameAppFunction,
                             (tempHostnameAppFunction$eventName == "Render" &
                                tempHostnameAppFunction$eventType == "STOP"))
  
  # Tiling Event name
  
  startAppDataTiling = subset(tempHostnameAppFunction,
                              (tempHostnameAppFunction$eventName == "Tiling" &
                                 tempHostnameAppFunction$eventType == "START"))
  stopAppDataTiling = subset(tempHostnameAppFunction,
                             (tempHostnameAppFunction$eventName == "Tiling" &
                                tempHostnameAppFunction$eventType == "STOP"))
  
  # Uploading Event name
  
  startAppDataUploading = subset(tempHostnameAppFunction,
                                 (tempHostnameAppFunction$eventName == "Uploading" &
                                    tempHostnameAppFunction$eventType == "START"))
  stopAppDataUploading = subset(tempHostnameAppFunction,
                                (tempHostnameAppFunction$eventName == "Uploading" & 
                                   tempHostnameAppFunction$eventType == "STOP"))
  
  
  # Set up some empty data frame to accommodate each event name
  
  totalRenderTemp = data.frame()
  savingConfigTemp = data.frame()
  renderTemp = data.frame()
  tilingTemp = data.frame()
  uploadingTemp = data.frame()
  
  # Looping the Task Id
  
  # Extract the task Id list from the application Data for the current loop Hostname
  taskIdList = unique(tempHostnameAppFunction$taskId)
  
  for (selectTaskF2 in taskIdList){
    
    # Set up empty vectors to store that would be refresh every loop for the
    # GPU condition during the task
    
    # Get the gpuSerial and gpuUUID from the gpu data
    
    
    # Total Render event name (The whole Task itself)
    tasklistloopTotalR = data.frame()
    tasklistloopStartTotalR = data.frame()
    tasklistloopStopTotalR = data.frame()
    gpudataTotalR = data.frame()
    
    
    tasklistloopStartTotalR = subset(startAppDataTotalRender,taskId == selectTaskF2)
    tasklistloopStopTotalR = subset(stopAppDataTotalRender,taskId == selectTaskF2)
    # Extracting the start and stop time of the task
    taskStart = tasklistloopStartTotalR$timestamp_H_M_S
    taskStop = tasklistloopStopTotalR$timestamp_H_M_S
    # Create a new feature called duration
    duration_Total_Render = as.duration(taskStop - taskStart)
    # Extract the GPU usage for the task
    gpudataTotalR = subset(tempHostnameGPUFunction, timestamp_H_M_S >= tasklistloopStartTotalR$timestamp_H_M_S
                           & timestamp_H_M_S <= tasklistloopStopTotalR$timestamp_H_M_S)
    
    # Column Bind
    tasklistloopTotalR = cbind(tasklistloopStartTotalR[,c(2,5,6)],taskStart,taskStop,duration_Total_Render)
    # Extract the GPU UUID
    tasklistloopTotalR[,"gpuUUID"] = unique(gpudataTotalR$gpuUUID)
    # Extract the GPU serial number
    tasklistloopTotalR[,"gpuSerial"] = unique(gpudataTotalR$gpuSerial)
    # Extract the average power draw (Watt) of the GPU during the task
    tasklistloopTotalR[,"totalAvgPowerDrawWatt"] = mean(gpudataTotalR$powerDrawWatt)
    # Extract the average Temperature (Celcius) of the GPU during the task
    tasklistloopTotalR[,"totalAvgGPUTempC"] = mean(gpudataTotalR$gpuTempC)
    # Extract the average Utility Percentage of the GPU during the task
    tasklistloopTotalR[,"totalAvgGPUUtilPerc"] = mean(gpudataTotalR$gpuUtilPerc)
    # Extract the average Memory Utility Percentage of the GPU during the
    tasklistloopTotalR[,"totalAvgGPUMemUtilPerc"] = mean(gpudataTotalR$gpuMemUtilPerc)
    
    # For each loop store the Data for each task list in a temporary storage
    totalRenderTemp = rbind(totalRenderTemp,tasklistloopTotalR)
    
    
    # Saving Configuration
    # Set up the empty data frames that would be refreshed every task list loop
    tasklistloopSaving = data.frame()
    tasklistloopStartSaving = data.frame()
    tasklistloopStopSaving = data.frame()
    # Empty data frame to store the initial gpu condition
    gpudataSavingInt = data.frame()
    # If it's empty set up higher limit data and lower limit data frames
    gpudataSavingHigh = data.frame()
    gpudataSavingLow = data.frame()
    
    # Empty data frame to store the final version of the gpu condition data
    gpudataSaving = data.frame()
    
    tasklistloopStartSaving = subset(startAppDataSavingConfig, taskId == selectTaskF2)
    tasklistloopStopSaving = subset(stopAppDataSavingConfig, taskId == selectTaskF2)
    # Extracting the start and stop time of saving config
    savingConfigStart = tasklistloopStartSaving$timestamp_H_M_S
    savingConfigStop = tasklistloopStopSaving$timestamp_H_M_S
    # Create a duration feature for Saving Config event name
    duration_Saving_Config =  as.duration(savingConfigStop - savingConfigStart)
    # Extract the GPU usage data for the Saving Config event name
    gpudataSavingInt = subset(tempHostnameGPUFunction, timestamp_H_M_S >= tasklistloopStartSaving$timestamp_H_M_S 
                              & timestamp_H_M_S <= tasklistloopStopSaving$timestamp_H_M_S)
    
    # Because for this particular event, it is < 1 second, there might be a lot of
    # data individuals that have missing values, therefore we take the 2 closest 
    # timestamp values. 1 for the low limit(START timestamp) and 1 for the high limit
    # (STOP timestamp)
    
    if(nrow(gpudataSavingInt) == 0){
      # Low Boundary data
      gpudataSavingLow = subset(tempHostnameGPUFunction,timestamp_H_M_S <= savingConfigStart)
      # Upper Boundary data
      gpudataSavingHigh = subset(tempHostnameGPUFunction, timestamp_H_M_S >= savingConfigStop)
      
      # Extract the Lower Limit (Closest Timestamp to Start of the Saving Config Event)
      lowlimitHourSaving = max(hour(gpudataSavingLow$timestamp_H_M_S))
      lowlimitHourSavingData = subset(gpudataSavingLow, hour(gpudataSavingLow$timestamp_H_M_S)
                                      == lowlimitHourSaving)
      lowlimitHourMinSaving = max(minute(lowlimitHourSavingData$timestamp_H_M_S))
      lowlimitHourMinSavingData = subset(lowlimitHourSavingData, minute(lowlimitHourSavingData$timestamp_H_M_S) == 
                                           lowlimitHourMinSaving)
      lowlimitHourMinSecSaving = max(second(lowlimitHourMinSavingData$timestamp_H_M_S))
      lowlimitHourMinSecSavingData = subset(lowlimitHourMinSavingData, second(lowlimitHourMinSavingData$timestamp_H_M_S)
                                            == lowlimitHourMinSecSaving)
      
      # Extract the Upper Limit (Closest Timestamp to Stop of the Saving Config Event)
      highlimitHourSaving = min(hour(gpudataSavingHigh$timestamp_H_M_S))
      highlimitHourSavingData = subset(gpudataSavingHigh, hour(gpudataSavingHigh$timestamp_H_M_S)
                                       == highlimitHourSaving)
      highlimitHourMinSaving = min(minute(highlimitHourSavingData$timestamp_H_M_S))
      highlimitHourMinSavingData = subset(highlimitHourSavingData, minute(highlimitHourSavingData$timestamp_H_M_S) == 
                                            highlimitHourMinSaving)
      highlimitHourMinSecSaving = min(second(highlimitHourMinSavingData$timestamp_H_M_S))
      highlimitHourMinSecSavingData = subset(highlimitHourMinSavingData, second(highlimitHourMinSavingData$timestamp_H_M_S)
                                             == highlimitHourMinSecSaving)
      # Combine the two limits of the data in order to get the interpolation of the gpu condition between those 2 limits
      gpudataSaving = rbind(lowlimitHourMinSecSavingData,highlimitHourMinSecSavingData)
      
    }else{
      
      gpudataSaving = gpudataSavingInt
      
    }
    
    # Column Bind the Duration, and the GPU conditions with the hostname, jobId, and taskId
    tasklistloopSaving = cbind(tasklistloopStartSaving[,c(2,5,6)],savingConfigStart,savingConfigStop,
                               duration_Saving_Config)
    
    
    # Extract the average power draw in Watt for the Saving Config event name
    tasklistloopSaving[,"savingAvgPowerDrawWatt"] = mean(gpudataSaving$powerDrawWatt)
    # Extract the average GPU temperatrue (Celcius) for the Saving Config event name
    tasklistloopSaving[,"savingAvgGPUtempC"] = mean(gpudataSaving$gpuTempC)
    # Extract the average GPU Utility Percentage for the saving config event name
    tasklistloopSaving[,"savingAvgGPUUtilPerc"] = mean(gpudataSaving$gpuUtilPerc)
    # Extract the average GPU Memory Utility Percentage for the saving config event name
    tasklistloopSaving[,"savingAvgGPUMemUtilPerc"] = mean(gpudataSaving$gpuMemUtilPerc)
    # Store the data from each task list loop into the temporary storage
    savingConfigTemp = rbind(savingConfigTemp, tasklistloopSaving)
    
    
    # Render event name
    # Set up empty vectors to store that would be refresh every loop for the
    
    tasklistloopRender = data.frame()
    tasklistloopStartRender = data.frame()
    tasklistloopStopRender = data.frame()
    gpudataRender = data.frame()
    
    
    tasklistloopStartRender = subset(startAppDataRender,taskId == selectTaskF2)
    tasklistloopStopRender = subset(stopAppDataRender,taskId == selectTaskF2)
    # Extracting the start and stop time of the task
    renderStart = tasklistloopStartRender$timestamp_H_M_S
    renderStop = tasklistloopStopRender$timestamp_H_M_S
    # Create a new feature called duration
    duration_Render = as.duration(renderStop - renderStart)
    # Extract the GPU usage for the task
    gpudataRender = subset(tempHostnameGPUFunction, timestamp_H_M_S >= tasklistloopStartRender$timestamp_H_M_S
                           & timestamp_H_M_S <= tasklistloopStopRender$timestamp_H_M_S)
    
    # Column Bind
    tasklistloopRender = cbind(tasklistloopStartRender[,c(2,5,6)],renderStart,renderStop,duration_Render)
    # Extract the average power draw (Watt) of the GPU during the task
    tasklistloopRender[,"renderAvgPowerDrawWatt"] = mean(gpudataRender$powerDrawWatt)
    # Extract the average Temperature (Celcius) of the GPU during the task
    tasklistloopRender[,"renderAvgGPUTempC"] = mean(gpudataRender$gpuTempC)
    # Extract the average Utility Percentage of the GPU during the task
    tasklistloopRender[,"renderAvgGPUUtilPerc"] = mean(gpudataRender$gpuUtilPerc)
    # Extract the average Memory Utility Percentage of the GPU during the
    tasklistloopRender[,"renderAvgGPUMemUtilPerc"] = mean(gpudataRender$gpuMemUtilPerc)
    
    # For each loop store the Data for each task list in a temporary storage
    renderTemp = rbind(renderTemp,tasklistloopRender)
    
    
    # Tiling event name
    # Set up empty data frames that would be refreshed every task list loop
    tasklistloopTiling = data.frame()
    tasklistloopStartTiling = data.frame()
    tasklistloopStopTiling = data.frame()
    # Setting up empty Initial data frame for gpu condition
    gpudataTilingInt = data.frame()
    # If the initial data frame is empty after applying the conditional, set up
    # high and low limit
    gpudataTilingLow = data.frame()
    gpudataTilingHigh = data.frame()
    
    gpudataTiling = data.frame()
    # Subset the data based on the task id
    tasklistloopStartTiling = subset(startAppDataTiling, taskId == selectTaskF2)
    tasklistloopStopTiling = subset(stopAppDataTiling, taskId == selectTaskF2)
    # Extract the start time and the stop time of the Tiling event name
    tilingStart = tasklistloopStartTiling$timestamp_H_M_S
    tilingStop = tasklistloopStopTiling$timestamp_H_M_S
    # Extract the duration of the Tiling event which is Stop - Start
    duration_Tiling = as.duration(tilingStop - tilingStart)
    # Extract the GPU conditions during this event 
    gpudataTilingInt = subset(tempHostnameGPUFunction,timestamp_H_M_S >= tasklistloopStartTiling$timestamp_H_M_S 
                              & timestamp_H_M_S <= tasklistloopStopTiling$timestamp_H_M_S)
    
    if(nrow(gpudataTilingInt) == 0){
      # Set the lower limit Data
      gpudataTilingLow = subset(tempHostnameGPUFunction, timestamp_H_M_S <= tilingStart)
      # Set the upper limit Data
      gpudataTilingHigh = subset(tempHostnameGPUFunction, timestamp_H_M_S >= tilingStop)
      
      # Extract the Closest timestamp to the START Tiling timestamp
      lowlimitHourTiling = max(hour(gpudataTilingLow$timestamp_H_M_S))
      lowlimitHourTilingData = subset(gpudataTilingLow, hour(gpudataTilingLow$timestamp_H_M_S) == lowlimitHourTiling)
      lowlimitHourMinTiling = max(minute(lowlimitHourTilingData$timestamp_H_M_S))
      lowlimitHourMinTilingData = subset(lowlimitHourTilingData, 
                                         minute(lowlimitHourTilingData$timestamp_H_M_S) 
                                         == lowlimitHourMinTiling)
      lowlimitHourMinSecTiling = max(second(lowlimitHourMinTilingData$timestamp_H_M_S))
      lowlimitHourMinSecTilingData = subset(lowlimitHourMinTilingData, 
                                            second(lowlimitHourMinTilingData$timestamp_H_M_S) 
                                            == lowlimitHourMinSecTiling)
      
      # Extract the Closest timestamp to the STOP of Tiling timestamp
      highlimitHourTiling = min(hour(gpudataTilingHigh$timestamp_H_M_S))
      highlimitHourTilingData = subset(gpudataTilingHigh, hour(gpudataTilingHigh$timestamp_H_M_S) == highlimitHourTiling)
      highlimitHourMinTiling = min(minute(highlimitHourTilingData$timestamp_H_M_S))
      highlimitHourMinTilingData = subset(highlimitHourTilingData, 
                                          minute(highlimitHourTilingData$timestamp_H_M_S) 
                                          == highlimitHourMinTiling)
      highlimitHourMinSecTiling = min(second(highlimitHourMinTilingData$timestamp_H_M_S))
      highlimitHourMinSecTilingData = subset(highlimitHourMinTilingData, 
                                             second(highlimitHourMinTilingData$timestamp_H_M_S) == 
                                               highlimitHourMinSecTiling)
      gpudataTiling = rbind(highlimitHourMinSecTilingData, lowlimitHourMinSecTilingData)
      
      
      
    }else{
      
      gpudataTiling = gpudataTilingInt
      
    }
    
    
    # Column Bind
    tasklistloopTiling = cbind(tasklistloopStartTiling[,c(2,5,6)],tilingStart,tilingStop,
                               duration_Tiling)
    # Extract the average power draw in Watt for the Tiling event name
    tasklistloopTiling[,"tilingAvgPowerDrawWatt"] = mean(gpudataTiling$powerDrawWatt)
    # Extract the average GPU temperature (Celcius) for the Tiling event name
    tasklistloopTiling[,"tilingAvgGPUTempC"] = mean(gpudataTiling$gpuTempC)
    # Extract the average Percent Utilisation of GPU Cores for the Tiling event
    tasklistloopTiling[,"tilingAvgGPUUtilPerc"] = mean(gpudataTiling$gpuUtilPerc)
    # Extract the average Percent Utilisation of GPU Memory for the Tiling event
    tasklistloopTiling[,"tilingAvgGPUMemUtilPerc"] = mean(gpudataTiling$gpuMemUtilPerc)
    # Store the data from the task list loop into the temporary storage
    tilingTemp = rbind(tilingTemp, tasklistloopTiling)
    
    
    # Uploading event name
    # Set up data frames that would be refreshed every task list loop
    tasklistloopUploading = data.frame()
    tasklistloopStartUploading = data.frame()
    tasklistloopStopUploading = data.frame()
    # Set up the initial gpu condition data frame
    gpudataUploadingInt = data.frame()
    # If the inital gpu condition data frame is empty after applying the conditional
    # set up upper and lower limit of the gpu condition data to be interpolated 
    gpudataUploadingHigh = data.frame()
    gpudataUploadingLow = data.frame()
    
    gpudataUploading = data.frame()
    # Subset the data based on the task id
    tasklistloopStartUploading = subset(startAppDataUploading,taskId == selectTaskF2)
    tasklistloopStopUploading = subset(stopAppDataUploading, taskId == selectTaskF2)
    # Extract the Start and Stop time
    uploadingStart = tasklistloopStartUploading$timestamp_H_M_S
    uploadingStop = tasklistloopStopUploading$timestamp_H_M_S
    duration_Uploading = as.duration(uploadingStop - uploadingStart)
    # Extract the GPU condition during this event
    gpudataUploadingInt = subset(tempHostnameGPUFunction,timestamp_H_M_S >= tasklistloopStartUploading$timestamp_H_M_S 
                                 & timestamp_H_M_S <= tasklistloopStopUploading$timestamp_H_M_S)
    
    # Applying a conditional that if using the conditional above, the data return
    # an empty list
    
    if (nrow(gpudataUploadingInt) == 0){
      # Set the lower limit data
      gpudataUploadingLow = subset(tempHostnameGPUFunction, timestamp_H_M_S <= uploadingStart)
      # Set the upper limit data
      gpudataUploadingHigh = subset(tempHostnameGPUFunction, timestamp_H_M_S >= uploadingStop)
      
      # Extract the gpu condition that have the closest timestamps to the two limits above
      lowlimitHourUploading = max(hour(gpudataUploadingLow$timestamp_H_M_S))
      lowlimitHourUploadingData = subset(gpudataUploadingLow, 
                                         hour(gpudataUploadingLow$timestamp_H_M_S) 
                                         == lowlimitHourUploading)
      lowlimitHourMinUploading = max(minute(lowlimitHourUploadingData$timestamp_H_M_S))
      lowlimitHourMinUploadingData = subset(lowlimitHourUploadingData, 
                                            minute(lowlimitHourUploadingData$timestamp_H_M_S) 
                                            == lowlimitHourMinUploading)
      lowlimitHourMinSecUploading = max(second(lowlimitHourMinUploadingData$timestamp_H_M_S))
      lowlimitHourMinSecUploadingData = subset(lowlimitHourMinUploadingData, 
                                               second(lowlimitHourMinUploadingData$timestamp_H_M_S) 
                                               == lowlimitHourMinSecUploading)
      
      highlimitHourUploading = min(hour(gpudataUploadingHigh$timestamp_H_M_S))
      highlimitHourUploadingData = subset(gpudataUploadingHigh, 
                                          hour(gpudataUploadingHigh$timestamp_H_M_S) 
                                          == highlimitHourUploading)
      highlimitHourMinUploading = min(minute(highlimitHourUploadingData$timestamp_H_M_S))
      highlimitHourMinUploadingData = subset(highlimitHourUploadingData, 
                                             minute(highlimitHourUploadingData$timestamp_H_M_S)
                                             == highlimitHourMinUploading)
      highlimitHourMinSecUploading = min(second(highlimitHourMinUploadingData$timestamp_H_M_S))
      highlimitHourMinSecUploadingData = subset(highlimitHourMinUploadingData,
                                                second(highlimitHourMinUploadingData$timestamp_H_M_S)
                                                == highlimitHourMinSecUploading)
      gpudataUploading = rbind(lowlimitHourMinSecUploadingData, highlimitHourMinSecUploadingData)
      
      
    }else{
      
      gpudataUploading = gpudataUploadingInt
      
    }
    
    # Column Bind the data
    tasklistloopUploading = cbind(tasklistloopStartUploading[,c(2,5,6)],uploadingStart,uploadingStop,
                                  duration_Uploading)
    # Extract the average power draw in Watt for the Uploading event name
    tasklistloopUploading[,"uploadingAvgPowerDrawWatt"] = mean(gpudataUploading$powerDrawWatt)
    # Extract the average GPU Temperature in Celciuse for the Uploading event name
    tasklistloopUploading[,"uploadingAvgGPUTempC"] = mean(gpudataUploading$gpuTempC)
    # Extract the average Percent Utilisation of GPU cores for the Uploading event name
    tasklistloopUploading[,"uploadingAvgGPUUtilPerc"] = mean(gpudataUploading$gpuUtilPerc)
    # Extract the average Percent Utilisation of GPU Memory for the Uploading event name
    tasklistloopUploading[,"uploadingAvgGPUMemUtilPerc"] = mean(gpudataUploading$gpuMemUtilPerc)
    # Store the data from the task list loop into the temporary storage
    uploadingTemp = rbind(uploadingTemp,tasklistloopUploading)
    
    
    # End of the tasklist loop
  }
  # Use the merge function to merge all the event names into 1 concatenated data
  # First merge (Total_Render and Saving Config )
  merge1TS = merge(totalRenderTemp,savingConfigTemp, by = c("hostname","jobId","taskId"))
  
  # Second Merge (Result of First Merge and Render)
  merge2TSR = merge(merge1TS, renderTemp, by = c("hostname","jobId","taskId"))
  
  # Third Merge (Result of Second Merge and Tiling)
  merge3TSRT = merge(merge2TSR, tilingTemp, by = c("hostname", "jobId", "taskId"))
  
  # Final Merge (Result of Third Merge and Uploading)
  merge4TSRTU = merge(merge3TSRT, uploadingTemp, by = c("hostname", "jobId", "taskId"))
  
  finalData_2 = rbind(finalData_2,merge4TSRTU)
  
  # End of the Hostname Loop
}

cache('finalData_2')

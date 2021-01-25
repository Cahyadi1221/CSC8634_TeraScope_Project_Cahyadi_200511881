# Pre-Processing Script To wrangled the data

# To avoid duplicates, create a distinct data set using the unique function

# Application Checkpoint Data without duplicates
appCheckpointRawDistinct = unique(Copy.of.application.checkpoints)

# GPU data without duplicates
gpuRawDistinct = unique(Copy.of.gpu)


# Save the data frames in new variables for ease of use
appDataframe = appCheckpointRawDistinct
GPUdataframe = gpuRawDistinct

hostnameListF = unique(appDataframe$hostname)
finalData = data.frame()

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
    gpudataSaving = subset(tempHostnameGPUFunction, timestamp_H_M_S >= tasklistloopStartSaving$timestamp_H_M_S 
                           & timestamp_H_M_S <= tasklistloopStopSaving$timestamp_H_M_S)
    
    
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
    gpudataTiling = subset(tempHostnameGPUFunction,timestamp_H_M_S >= tasklistloopStartTiling$timestamp_H_M_S 
                           & timestamp_H_M_S <= tasklistloopStopTiling$timestamp_H_M_S)
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
    gpudataUploading = data.frame()
    # Subset the data based on the task id
    tasklistloopStartUploading = subset(startAppDataUploading,taskId == selectTaskF2)
    tasklistloopStopUploading = subset(stopAppDataUploading, taskId == selectTaskF2)
    # Extract the Start and Stop time
    uploadingStart = tasklistloopStartUploading$timestamp_H_M_S
    uploadingStop = tasklistloopStopUploading$timestamp_H_M_S
    duration_Uploading = as.duration(uploadingStop - uploadingStart)
    # Extract the GPU condition during this event
    gpudataUploading = subset(tempHostnameGPUFunction,timestamp_H_M_S >= tasklistloopStartUploading$timestamp_H_M_S 
                              & timestamp_H_M_S <= tasklistloopStopUploading$timestamp_H_M_S)
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
  
  finalData = rbind(finalData,merge4TSRTU)
  
  # End of the Hostname Loop
}
# Save the data in the cache file so that to reproduce the analysis, the user 
# should not have to wait for the whole process which took around 20 mins
cache('finalData')

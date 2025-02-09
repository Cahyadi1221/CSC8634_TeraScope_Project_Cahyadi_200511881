# Analysis Script
library(ProjectTemplate)
library(lubridate)
library(GGally)
library(knitr)
load.project()

# Check the duplicates on the original application checkpoints data

applicationCheckpointDuplicates = anyDuplicated(Copy.of.application.checkpoints)
# Run the variable to check the number of duplicates in the original application checkpoint 
# data
applicationCheckpointDuplicates
# There are 130821 duplicates within this data set

# Check the duplicates for the gpu data
gpuDataDuplicates = anyDuplicated(Copy.of.gpu)
# Run the Variable to check the numer of duplicates on the gpu data
gpuDataDuplicates

# Check the GPU UUID
length(unique(Copy.of.gpu$gpuUUID))

# Therefore in the wrangling process, the usage of unique(data) to avoid processing 
# duplicates is validated. 


# Numerical Summaries of the horizontal data
cor(masterData[,c(6,9:12,15:19,22:26,29:33,36:43)])


# Pairs function on the vertical data

ggpairs(eventNamesData, columns = c("duration","AvgPowerDrawWatt", "AvgGPUTempC", "AvgGPUUtilPerc", "AvgGPUMemUtilPerc" ), mapping = ggplot2::aes(colour = eventName))

# Get the correlation matrix from the numerical columns
cor(eventNamesData[,c(7,10:12)])


# Subsetting the data via event Name and check the Correlation Matrix for each

# Total Render

# Numerical Summaries
cor(totalRenderData[,c(7,10:13,16)])
# Extracting the ColMeans for the data set
numericalAvgTotalRender = colMeans(totalRenderData[,c(7,10:13)])
numericalAvgTotalRender

# Saving Config
cor(savingConfigData[,c(7,10:13)])
# The correlation between the duration and the Power Draw is really low, quite
# possibly caused by the interpolation made to record the GPU condition of this
# particular event name
# Extracting the ColMeans for the data set
numericalAvgSavingConfig = colMeans(savingConfigData[,c(7,10:13)])
numericalAvgSavingConfig
# The saving Config Event has an average
# Duration          = 0.002476266 seconds
# Power Draw        = 41.166747222 Watt
# GPU Temp          = 38.330681075 Celcius
# GPU Util Perc     = 5.024721475 %
# GPU Mem Util Perc = 2.425903972 %


# Render 
cor(renderData[,c(7,10:13,16)])
# The correlation matrix here provide a similar result to that of the total Render
# event name. This is probably due to the characteristic of the render event name
# in which it dominates the duration of the task itself

# Extracting the ColMeans for the data set
numericalAvgRender = colMeans(renderData[,c(7,10:13)])
numericalAvgRender
# The Render event has an average 
# Duration          = 41.20822 seconds
# Power Draw        = 95.78656 Watt
# GPU Temp          = 40.54942 Celcius
# GPU Util Perc     = 71.44715 %
# GPU Mem Util Perc = 37.43622 %

# Tiling
cor(tilingData[,c(7,10:13)])
# Again, having the similar correlation matrix result with the Saving Config 
# event name, quite possibly because we are using the interpolation of gpu condition
# of the 2 closest timestamp

# Extracting the colMeans for the data set
numericalAvgTiling = colMeans(tilingData[,c(7,10:13)])
numericalAvgTiling
# The Tiling event has an average 
# Duration          = 0.9732072 seconds
# Power Draw        = 50.4252683 Watt
# GPU Temp          = 39.5941514 Celcius
# GPU Util Perc     = 12.8614442 %
# GPU Mem Util Perc = 6.0049473 %


# Uploading
cor(uploadingData[,c(7,10:13)])

# Extracting the colMeans for the data set
numericalAvgUploading = colMeans(uploadingData[,c(7,10:13)])
numericalAvgUploading

# The Tiling event has an average 
# Duration          = 1.393641 seconds
# Power Draw        = 49.845835 Watt
# GPU Temp          = 39.533123 Celcius
# GPU Util Perc     = 12.323613 %
# GPU Mem Util Perc = 5.750262 %

# Assessing individual event name
# Since Render is the event that dominate a rendering task, we focus on render

# Create a correlation matrix 
cor(eventNamesData[,c(7,10:13,16:18)])

# Extract the variance of the data
diag(var(eventNamesData[,c(7,10:13,16:18)]))

# It seems that the data is highly variated and it is reflected by their variance
summary(eventNamesData[,c(7,10:13,16:18)])

# Check whether there is variation of computational requirement for different level of zoom

# Extract only data points that are rendered on level 4
level4Data = subset(totalRenderData, level == 4)
# Extract only data points that are rendered on level 8
level8Data = subset(totalRenderData, level ==8)
# Extract only data points that are rendered on level 12
level12Data = subset(totalRenderData, level ==12)

# Since there is only 1 image tile which is rendered at level 4 zoom, therefore 
# for the level 4 zoom level we could only use this data point as reference

# We would combine the 3 level as 1 data using row bind
firstTileLeveledData = rbind(level4Data[,c(7,9:16)], 
                           subset(level8Data, x == 0 & y == 0)[,c(7,9:16)], 
                           subset(level12Data,x == 0 & y == 0)[,c(7,9:16)])
cache('firstTileLeveledData')


# Subset image tile that are both present in the level 8 zoom and level 12 zoom
level12and8 = subset(level12Data, x <=15 & y <= 15)

# Extract only the average time for each 
level8 = colMeans(level8Data[,c(7,10:13,16)])
level12 = colMeans(level12and8[,c(7,10:13,16)])
comparison12and8 = rbind(level8,level12)
comparison12and8 = as.data.frame(comparison12and8)
cache('comparison12and8')

# Based on this data frame, the level seems affecting the performance of the GPU
# the more the image is zoomed out the more computational power it required


# Finding the inefficiency of task scheduling within each host
# Subset the Total Render from the Application Checkpoints Duration data frame
totalRenderDuration = subset(Durations, eventName == "TotalRender")

# Extract the hostname list
hostnameList = unique(totalRenderDuration$hostname)
lengthOfRecording = vector()
totalIdleTime = vector()
# 
for(host in hostnameList){
  
  totalRenderDurationHost = subset(totalRenderDuration,hostname == host)
  # Order the data frame
  totalRenderDurationHost = totalRenderDurationHost[order(totalRenderDurationHost$START),]
  timeLoss = 0 
  for(r in 1:(nrow(totalRenderDurationHost)-1)){
  
    gap = 0
    gap = as.duration(totalRenderDurationHost$START[r+1] - totalRenderDurationHost$STOP[r])
    timeLoss = timeLoss + gap
  
  }
# Calculate the timelapse of the recording from the first task until the last task
# that are recorded
  timelapsed = totalRenderDurationHost$STOP[nrow(totalRenderDurationHost)] - totalRenderDurationHost$START[1]
  lengthOfRecording = append(lengthOfRecording, timelapsed)
  totalIdleTime = append(totalIdleTime,timeLoss)
}
# Create a new data frame to show the inefficiencies for each hostname
inefficiencyData = data.frame(
  Hostname = hostnameList, TimeLoss = totalIdleTime,RecordedLength = lengthOfRecording )

# Cache the data frame 
cache('inefficiencyData')



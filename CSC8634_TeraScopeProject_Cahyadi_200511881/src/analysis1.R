# Analysis Script
library(dplyr)
library(ProjectTemplate)
library(lubridate)
library(ggplot2)
load.project()

# Check the duplicates on the original data

applicationCheckpointDuplicates = anyDuplicated(Copy.of.application.checkpoints)
# Run the variable to check the number of duplicates in the original data
applicationCheckpointDuplicates
# There are 130821 duplicates within this data set

# Therefore in the wrangling process, the usage of unique(data) to avoid processing 
# duplicates is validated. 

# Check for the total Render data, the correlation using pairs function

graphicalSummaries1 = pairs(masterData[,c(6,9:12)], main = "Total Render Plots")
cache('graphicalSummaries1')

# Numerical Summaries of the horizontal data
cor(masterData[,c(6,9:12,15:19,22:26,29:33,36:43)])

# Graphical summaries on the vertical data
pairs(verticalData_Raw[,c(7,10:12)], main = "All Event Name Pairs Plot")

# Get the correlation matrix from the numerical columns
cor(verticalData_Raw[,c(7,10:12)])

# Subsetting the data via event Name and check the Correlation Matrix for each

# Total Render
totalRenderData = subset(verticalData_Raw, eventName == "TotalRender")
cor(totalRenderData[,c(7,10:13)])
# Extracting the ColMeans for the data set
numericalAvgTotalRender = colMeans(totalRenderData[,c(7,10:13)])
numericalAvgTotalRender

# Saving Config
savingConfigData = subset(verticalData_Raw, eventName == "Saving Config")
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
renderData = subset(verticalData_Raw, eventName == "Render")
cor(renderData[,c(7,10:13)])
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
tilingData  = subset(verticalData_Raw, eventName == "Tiling")
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
uploadingData = subset(verticalData_Raw, eventName == "Uploading")
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





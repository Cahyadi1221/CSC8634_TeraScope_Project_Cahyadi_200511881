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

graphicalSummaries1 = pairs(masterData[,c(6,9:12)])
cache('graphicalSummaries1')

# Numerical Summaries of the horizontal data
cor(masterData[,c(6,9:12,15:19,22:26,29:33,36:43)])

# Graphical summaries on the vertical data
pairs(verticalData_Raw[,c(7,10:12)])

# Get the correlation matrix from the numerical columns
cor(verticalData_Raw[,c(7,10:12)])

# Subsetting the data via event Name and check the Correlation Matrix for each

# Total Render
totalRenderData = subset(verticalData_Raw, eventName == "TotalRender")
cor(totalRenderData[,c(7,10:13)])

# Saving Config
savingConfigData = subset(verticalData_Raw, eventName == "Saving Config")
cor(savingConfigData[,c(7,10:13)])
# The correlation between the duration and the Power Draw is really low, quite
# possibly caused by the interpolation made to record the GPU condition of this
# particular event name

# Render 
renderData = subset(verticalData_Raw, eventName == "Render")
cor(renderData[,c(7,10:13)])


# Tiling
tilingData  = subset(verticalData_Raw, eventName == "Tiling")
cor(tilingData[,c(7,10:13)])

# Uploading
uploadingData = subset(verticalData_Raw, eventName == "Uploading")
cor(uploadingData[,c(7,10:13)])





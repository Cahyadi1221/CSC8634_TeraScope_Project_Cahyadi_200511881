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
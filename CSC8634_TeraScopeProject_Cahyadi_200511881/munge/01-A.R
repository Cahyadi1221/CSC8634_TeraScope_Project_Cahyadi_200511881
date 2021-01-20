# The First Pre-Processing Script of the Project
# This Script is mainly just to check for Data

# Check how many hostname for each data set

# First the application checkpoint data
length(unique(Copy.of.application.checkpoints$hostname))

# Second the gpu data
length(unique(Copy.of.gpu$hostname))

# For the task.x.y data it doesn't have any hostname only task id

# Since for both gpu and application data the hostname have the same length,
# we assume that for both data set the hostname is the same, but we can check using
# the setequal function of dplyr

setequal(unique(Copy.of.application.checkpoints$hostname),unique(Copy.of.gpu$hostname))

# This result is TRUE that means that for both the hostname are indeed the same

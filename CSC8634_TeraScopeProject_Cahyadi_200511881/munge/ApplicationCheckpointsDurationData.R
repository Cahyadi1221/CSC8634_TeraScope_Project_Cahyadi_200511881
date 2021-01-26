# Testing Pivot Wider

# Get the unique Application Checkpoints Data
applicationCheckpoints = unique(Copy.of.application.checkpoints)
# Extract only the time value out of the timestamp since the dates are showing the
# same data throughout the data and converting this extracted time value into 
# dplyr hms format
applicationCheckpoints[,"timestamp"] = hms(substr(applicationCheckpoints$timestamp,12,23))

# Create a new data frame called Durations that extract the duration of each event name
# by subtracting STOP and START timestamp.
Durations <- applicationCheckpoints %>%
  pivot_wider( 
    # Create new columns called STOP and START in which store the START time
    # of the event and the STOP time of the event
    names_from = eventType,
    values_from = timestamp
    
    )
# Create a new feature called duration.
Durations[,"duration"] = as.duration(Durations$STOP - Durations$START)
cache('Durations')

# Shall we need for horizontal data for analysis, create a new horizontal data 
# that only takes into account the duration, without the timestamp

# Extract only the Hostname, TaskId, JobId, eventName, and duration column out 
# of the Durations data frame and use pivot_wider to create a horizontal 
# duration data
durationsWithoutTimestamp = Durations[,c(1:4,7)]
# Apply the pivot_wider function
horizontalDurations <- durationsWithoutTimestamp%>%
  pivot_wider(
    # Create new columns based on the distinct values of eventName
    names_from = eventName,
    # Fill these newly made columns with the duration for each event name
    values_from = duration
    
  )

# cache the newly made horizontal dataframe
cache('horizontalDurations')

# Testing Pivot Wider

# Get the unique Application Checkpoints Data
applicationCheckpoints = unique(Copy.of.application.checkpoints)
applicationCheckpoints[,"timestamp"] = hms(substr(applicationCheckpoints$timestamp,12,23))

Durations <- applicationCheckpoints %>%
  pivot_wider( 
    
    names_from = eventType,
    values_from = timestamp
    
    )
Durations[,"duration"] = as.duration(Durations$STOP - Durations$START)
cache('Durations')

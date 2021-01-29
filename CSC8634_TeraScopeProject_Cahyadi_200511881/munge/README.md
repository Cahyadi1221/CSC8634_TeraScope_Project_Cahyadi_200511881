# Munge Files Folder For Pre-Processing Data

&nbsp;&nbsp;&nbsp; This folder contains all the necessary scripts to wrangle and pre-process the data. Since the wrangling process would took a long time to run, the munging is set to FALSE. Instead, the results of the data pre-processing done in this munge folder has been cached. Shall the wrangling process needs to be reproduced below are the list of pre-processing scripts : 

  * 01-A.R -> Only used to perform initial check on the data frames
  * ApplicationCheckpointsDurationData.R -> Wrangle the application checkpoints data frame to extract the **duration** feature 
  * mergingAndSubsetting.R -> Perform merge on the three available data frames (application checkpoints, gpu, and task.x.y ) and subset the results of this merge by **Event Name**

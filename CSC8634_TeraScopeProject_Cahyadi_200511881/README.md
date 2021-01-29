# CSC8634_TeraScopeProject_Cahyadi_200511881

Welcome to **Performance Evaluation Of Terapixel Rendering in Cloud(Super) Computing Project**!

# Abstract
<details> <summary>...</summary><b>Context</b></details>  
&nbsp;&nbsp;&nbsp; As the demand and implementation of cloud technologies increased, subsequently there is an increase of interest in finding information for improving its efficiency through Performance evaluation. This project would extract such performance evaluation out of cloud architecture meant to produce Terapixel image of 3D city visualisation of Newcastle Upon Tyne.  
<details><summary>...</summary><b>Objective</b></details>  
&nbsp;&nbsp;&nbsp; The goal of this project is that performance evaluation should be able to pin point the area in which an optimisation should be focused on, and extract necessary performance information out of the current hardware and services utilised in the current cloud architecture.   
<details><summary>...</summary><b>Method</b></details>  
&nbsp;&nbsp;&nbsp; To achieve such goal, a data mining process would be conducted following a CRISP-DM best practice method. The main program in which the data mining process is done is Rstudio and further streamlined by using the Project Template package.  
<details><summary>...</summary><b>Results</b></details>  
&nbsp;&nbsp;&nbsp; Implementing these data mining process into the data we can extract the hardware performance information regarding on its capabilities to render an image tile. And the results of these analyses can provide answers to the questions necessary for evaluating the   performance of the current cloud-architecture  
<details><summary>...</summary><b>Novelty</b></details>  
&nbsp;&nbsp;&nbsp; This project is done entirely using the CRISP-DM methodology and focuses on creating an EDA on the data set in which, the results of this project serve as a foundation for future projects on the cloud-architecture meant to produce terapixel 3D City visualisation of Newcastle Upon Tyne.  

<img src= "complete_eventNamesPairsData.jpeg"/> 

# Project Template Description 

&nbsp;&nbsp;&nbsp; This project utilise **Project Template** as a way to streamlined the analyses process. Therefore the files necessary to run this project are stored within its designated location in which, the load.project() function would be able to call and run these files in an orderly manner. For better navigating below are the important files for this project and where to find them: 

  * **munge** folder -> Contains the pre-processing files which are used to wrangle the data to be used for analyses. To expedite the reproducibility process, the data munging option is disabled and instead, we would instead used cached data frame to reproduce the analyses.
  * **cache** folder -> Contains the cached data frames that could be used to reproduce the analyses done in this project.
  * **src** folder -> Contains the r script [analysis1.R](src/analysis1.R) that support the analyses.
  * **reports** folder -> Contains the Written report of this project and the structured abstract. 
  
# Steps to Reproduce the Analyses 

  1. Open R studio program.
  2. Select open project, and select CSC8634_TeraScopeProject_Cahyadi_200511881.Rproj from the main directory.
  3. Run the analysis1.R from src folder and writtenReport_Cahyadi_200511881.Rmd from reports folder simultaneosly.

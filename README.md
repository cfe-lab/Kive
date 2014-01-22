Shipyard
========

*Shipyard* is a set of Django applications for the archival and automation of bioinformatic pipelines and data sets.

Due to the cutting-edge nature of bioinformatics, large biological data sets such as genetic sequences are usually processed using a set of custom bioinformatic scripts, often referred to as a *pipeline*.  These scripts are constantly being debugged, modified and extended.  An enormous number of different versions of each script can rapidly accumulate, often with sporadic documentation or version control.  Consequently, **it is frequently impossible to determine how a given set of results were derived from the raw data.**

Revision control systems are great for tracking versions of your pipeline code (scripts), but they are seldom used to track versions of pipeline outputs (such as sequence assemblies).  More importantly, there is no convenient framework for linking these outputs to the versions of pipelines that generated them.  For example, labs tend to accumulate files with names like `project_results_20130119_corrected_FINAL_v2_fixed.csv`.

This lack of a system for linking results to pipeline versions makes it extremely difficult to make bioinformatic analyses reproducible or to disseminate methods to the research community.  Moreover, there is a growing demand for quality assurance in the development of bioinformatic pipelines, such as in a clinical setting.  

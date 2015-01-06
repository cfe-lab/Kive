Kive
====

*Kive* is a set of Django applications for the archival and automation of bioinformatic pipelines and data sets.

Due to the cutting-edge nature of bioinformatics, large biological data sets such as genetic sequences are usually processed using a set of custom bioinformatic scripts, often referred to as a *pipeline*.  These scripts are constantly being debugged, modified and extended.  An enormous number of different versions of each script can rapidly accumulate, often with sporadic documentation or version control.  Consequently, **it is frequently impossible to determine how a given set of results were derived from the raw data.**


Revision control for bioinformatics
-----------------------------------

Revision control systems are great for tracking versions of your pipeline code (scripts), but they are seldom used to track versions of pipeline outputs (such as sequence assemblies).  More importantly, there is no convenient framework for linking these outputs to the versions of pipelines that generated them.  For example, labs tend to accumulate files with names like `project_results_20130119_corrected_FINAL_v2_fixed.csv`.

This lack of a system for linking results to pipeline versions makes it extremely difficult to make bioinformatic analyses reproducible or to disseminate methods to the research community.  Moreover, there is a growing demand for quality assurance in the development of bioinformatic pipelines, such as in clinical settings.  After casting about for a solution, we decided to implement our own.


What does Kive do?
------------------

1. *Kive* keeps track of the different versions of your bioinformatic pipelines, down to the individual scripts that make them up.  It is NOT a full-featured revision control system.  It does not resolve conflicts between development branches, for example.  Kive should be used to keep track of the versions of your pipeline that graduate from *development* to *production*, where they are being used to analyze actual data sets.

2. Kive also keeps track of all the outputs at various stages of processing data through a pipeline.  We think of each step in a pipeline as a *transformation* of a data set from its raw state, through intermediate states, and ultimately to end results.  *Kive* records the MD5 checksum of each state as a permanent imprint of the data set contents.

What are we working on?
-----------------------

You can see active tasks on [our project board][waffle], or look at the [current milestone's burndown][burndown].

[waffle]: https://waffle.io/artpoon/shipyard
[burndown]: http://burndown.io/#ArtPoon/Shipyard


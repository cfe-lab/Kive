Kive
====

[![Build Status]][travis]
[![Code Coverage]][codecov]
[![DOI]][zenodo]


[Build Status]: https://github.com/cfe-lab/Kive/actions/workflows/build-and-test.yml/badge.svg?branch=master
[travis]: https://github.com/cfe-lab/Kive/actions
[Code Coverage]: https://codecov.io/github/cfe-lab/Kive/coverage.svg?branch=master
[codecov]: https://codecov.io/github/cfe-lab/Kive?branch=master
[DOI]: https://zenodo.org/badge/14132839.svg
[zenodo]: https://zenodo.org/badge/latestdoi/14132839

*Kive* is an accessible computing framework for the version control of
bioinformatic pipelines, along with their input and output datasets.

Background
----------
* Bioinformatic "pipelines" are collections of software programs that are used
  to process and analyze biological data.
* Pipelines have become essential tools in modern biomedical and clinical
  laboratories.
* Most pipelines are customized to meet the requirements of each lab and
  project. Therefore they are usually under constant development.
* The end-users are often unaware of revisions being made to pipelines.
* It can be difficult to determine which version of a pipeline was used to
  process a given data set, especially when there are multiple copies of results.
* This makes it difficult to reproduce results for method validation or
  publication.
* Clinical laboratory accreditation programs (such as the College of American
  Pathologists, CAP) have issued new requirements for the validation and
  version tracking of bioinformatic pipelines.
* A system for tracking this information should make it possible to look up the
  pipeline history of any data set. It should be easy to use, with an intuitive
  graphical interface, and with as much of the "bookkeeping" automated as
  possible. We could not find a system that met these criteria.


What does Kive do?
------------------
We developed our new framework ("Kive") as a Django application. Django is a
Python framework for developing web applications.

Kive is built on a PostgreSQL relational database. The database records the
digital "fingerprint" (md5 checksum) of every version of pipeline components and
data sets, their locations in the filesystem, and their relations to each other.

Executing a pipeline version on a data set is completely
automated by Kive, which distributes jobs across computing
resources (such as a computing cluster) and records every
intermediate step in the database. Any intermediate step
that can be re-used in subsequent pipeline versions will be
loaded to minimize computing time.

Read/write privileges to pipelines and data sets in Kive are
specific to users and groups.

Kive also features a web-based graphical user interface,
including a point-and-click toolkit for assembling and
running pipelines that is implemented in HTML5 Canvas and
JavaScript.

We used Kive to track versions of pipelines being developed
in-house for processing and interpreting raw data sets from
an Illumina MiSeq. This pipeline comprises 8 scripts written
in Python, Ruby, and R. For more information, read about how we fixed a problem
with [bad cycles in our example application][example].

[example]: http://cfe-lab.github.io/Kive/bad_cycles

Client requirements
-----------------------

The following browsers are supported

| Browser           | Basic Support | Bulk Upload Feature |
|-------------------|---------------|---------------------|
| Google Chrome     | version 4     | version 5           |
| Firefox           | version 4     | version 4           |
| Safari            | version 3.1   | version 7           |
| Internet Explorer | version 10    | version 10          |

RESTful API
-----------
You can upload data, launch pipelines, and update pipelines all through Kive's
API. You can also use our [Python library][kive-api] to script calls to the
API.

[kive-api]: https://github.com/cfe-lab/Kive/tree/master/api


What are we working on?
-----------------------

You can see active tasks on our [GitHub project].

[GitHub project]: https://github.com/cfe-lab/Kive/issues

Development
-----------

To upload a pipeline to Kive, read the [developer documentation].

To work on Kive itself, read about [installation], [contributing], and
look at the [model classes]. To look after a Kive server, follow the
[admin guide].

[developer documentation]: https://cfe-lab.github.io/Kive/dev_docs
[installation]: https://github.com/cfe-lab/Kive/blob/master/INSTALL.md
[contributing]: https://github.com/cfe-lab/Kive/blob/master/CONTRIBUTING.md
[model classes]: https://cfe-lab.github.io/Kive/models/README
[admin guide]: https://cfe-lab.github.io/Kive/admin
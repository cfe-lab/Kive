/* SQLEditor (SQLite)*/

/*Operations on datasets as a group, not particular revisions.*/

CREATE TABLE Methods
(
methodID INTEGER PRIMARY KEY  AUTOINCREMENT,
methodName TEXT NOT NULL UNIQUE,
methodDescription TEXT NOT NULL
);

CREATE TABLE ParentDatasets
(
datasetID_Parent INTEGER REFERENCES Datasets (datasetID),
datasetID INTEGER REFERENCES Datasets (datasetID),
revisionID INTEGER REFERENCES MethodRevision (revisionID)
);

/*pipelineDescriptionID: row number
pipelineID: group by pipeline

In general, we want to capture…

First sandbox contains inputs.

Step 1: Execute R1, make R1-2 and R1-3 available in the next sandbox.

Step 2: Execute R2, and make R2-1, R1-3, and INPUT-3 available in the next sandbox.

Step 3: Execute R3, make R3-2 available in the next sandbox.

Last sandbox contains things made available in the N-1 sandbox.*/

CREATE TABLE PipelineDescription
(
pipelineDescriptionID INTEGER PRIMARY KEY  AUTOINCREMENT,
revisionID INTEGER NOT NULL REFERENCES MethodRevision (revisionID),
pipelineID INTEGER NOT NULL UNIQUE,
step INTEGER NOT NULL
);

CREATE TABLE PipelineExecutionStatus
(
executionID INTEGER PRIMARY KEY,
sessionID INTEGER,
pipelineID INTEGER REFERENCES PipelineDescription (pipelineID),
step INTEGER
);

CREATE TABLE RevisionInput
(
revisionID INTEGER REFERENCES MethodRevision (revisionID),
inputOrder INTEGER NOT NULL,
inputFormat TEXT NOT NULL
);

CREATE TABLE RevisionOutput
(
revisionID INTEGER REFERENCES MethodRevision (revisionID),
outputOrder INTEGER NOT NULL,
outputFormat TEXT NOT NULL
);

CREATE TABLE RuntimeDatasetGeneration
(
datasetID INTEGER REFERENCES Datasets (datasetID),
executionID INTEGER REFERENCES PipelineExecutionStatus (executionID)
);

/*UserGroups

Constraint: All groups must have a name, and no two groups can have identical names.*/

CREATE TABLE UserGroups
(
userGroupID INTEGER PRIMARY KEY  AUTOINCREMENT,
groupName TEXT(100) NOT NULL UNIQUE
);

/*Users

1) Storing passwords as SHA-1 hash?

2) Emails must be unique*/

CREATE TABLE Users
(
userID INTEGER PRIMARY KEY  AUTOINCREMENT,
password CHAR(40) NOT NULL,
email TEXT(100) NOT NULL UNIQUE,
admin INTEGER
);

/*InGroup

Stores User <-> Group mappings

Constraints: Users can be assigned to groups 0 or 1 times.*/

CREATE TABLE InGroup
(
userGroupID INTEGER NOT NULL REFERENCES UserGroups (userGroupID),
userID INTEGER NOT NULL REFERENCES Users (userID)
);

CREATE TABLE DatasetAccess
(
groupID INTEGER NOT NULL REFERENCES UserGroups (userGroupID),
datasetID INTEGER NOT NULL REFERENCES Datasets (datasetID)
);

CREATE TABLE RevisionAccess
(
groupID INTEGER REFERENCES UserGroups (userGroupID),
revisionID INTEGER REFERENCES MethodRevision (revisionID)
);

CREATE TABLE Datasets
(
datasetID INTEGER PRIMARY KEY  AUTOINCREMENT,
datasetDescription TEXT NOT NULL,
MD5 INTEGER NOT NULL,
filepath TEXT NOT NULL,
datasetDate INTEGER NOT NULL,
deleteFlag INTEGER NOT NULL,
userID INTEGER REFERENCES Users (userID)
);

CREATE TABLE MethodRevision
(
revisionAuthor INTEGER REFERENCES Users (userID),
methodID INTEGER REFERENCES Methods (methodID),
revisionID INTEGER PRIMARY KEY  AUTOINCREMENT,
revisionID_Parent INTEGER REFERENCES MethodRevision (revisionID),
comment TEXT NOT NULL,
revisionPath TEXT NOT NULL,
scriptContents TEXT NOT NULL,
revisionDate INTEGER NOT NULL
);

CREATE UNIQUE INDEX Unique_user_group ON InGroup (userID,userGroupID);

CREATE UNIQUE INDEX DatasetAccess_idx ON DatasetAccess (groupID,datasetID);


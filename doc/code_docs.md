These documents describe the logic behind Shipyard code, what the
different classes are for, implicit assumptions, and so on.

Global Information
==================

complete\_clean()
-----------------
Several of Shipyard's models have a function called complete\_clean().
This function is intended to provide a superset of the checks that the
clean() function provides, in cases where a model may be coherent (ie.
pass clean()) but not complete. For example, when a user first drags a
Method into a Pipeline to form a new Pipelinestep, the step may be
coherent (eg. having at most one cable to each input), without being
complete (eg. there may be some inputs which do not yet have cables
defined to them). clean() only checks coherence; complete\_clean checks
coherence and completeness.

complete\_clean() must only be called on an object /after/ the object
has already been saved to the database. This is generally implicit in
the context in which complete\_clean() is called. Usually,
complete\_clean() on object A checks for the presence of some other
objects in the database which are related to A in some way, say by a
foreign key. This foreign key relation cannot exist until A has a
primary key - that is, A has been saved.


ExecLog
=======

An ExecLog is a record of code having been run during the execution of
one of the component parts of a Pipeline (ie. a PipelineStep,
PipelineStepInputCable, or PipelineOutputCable).

A single execution of a Pipeline in Shipyard is called a Run. When a Run
is created (that is, a Pipeline is executed), each of its steps and
cables are executed sequentially, creating the component RunSteps,
RunOutputCables, and RunStepInputCables of the Run (see the Archive and
Pipeline documentation for more details). Whenever one of these
components (a step or cable) is executed, it is possible (but not
necessary) that some code will have to be run on the system. When it
/is/ necessary, and code is run, a record of the code being executed is
created in an ExecLog. 

ExecLogs are only created for components of a Pipeline which are
/atomic/, meaning at most one code execution is needed to carry them
out. This includes steps which invoke individual Methods, and all
non-trivial cables which perform some transformation on the data they
are carrying. ExecLogs are not created for steps which invoke a whole
sub-Pipeline instead of a Method, as these steps are composed of many
components and are not atomic. Neither are they created for trivial
cables (which simply shunt data as-is from one step to another), because
these kinds of cables never require the execution of code.

Members
-------

### record

The Run component (ie. the RunStep, RunStepInputCable, or
RunOutputCable) which required code to be executed.

### start\_time

The exact date and time when execution of the code began. The start\_time
is automatically set when the ExecLog is created, and is therefore not
allowed to be null. You should never set this value manually.

### end\_time

The time when the execution of the code was finished. A null value of
this field means that the ExecLog pertains to an execution which is
still in progress. Once set, this value must be greater than start\_time.

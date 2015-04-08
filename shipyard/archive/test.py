import fleet.models
runs = list(fleet.models.RunToProcess.objects.select_related().
        prefetch_related('pipeline__steps',
                         'run__runsteps__log',
                         'run__runsteps__pipelinestep__cables_in',
                         'run__runsteps__pipelinestep__transformation__method',
                         'run__runsteps__pipelinestep__transformation__pipeline',
                         'run__pipeline__outcables__poc_instances__run',
                         'run__pipeline__outcables__poc_instances__log',
                         'run__pipeline__steps'))
st = []
for i,r in enumerate(runs):
    status = ""
    total_steps = r.run.pipeline.steps.count()
    runsteps = sorted(r.run.runsteps.all(), key=lambda rs: rs.pipelinestep.step_num)
    for j,s in enumerate(runsteps):
        print i,j
        if not s.is_complete_new():
            if hasattr(s, 'log'):
                status += "+"
            else:
                status += ":"
        elif not s.is_successful_new():
            status += "!"
        else:
            status += "*"
    # Just finished a step, but didn't start the next one?
    status += "." * (total_steps - len(runsteps))
    status += "-"
    # Which outcables are in progress?
    cables = r.run.pipeline.outcables.all()
    cables = sorted(cables, key=lambda x: x.output_idx)
    for pipeline_cable in cables:
        run_cables = filter(lambda x: x.run == r.run, pipeline_cable.poc_instances.all())
        if len(run_cables) <= 0:
            status += "."
        elif run_cables[0].is_complete_new():
            status += "*"
        else:
            try:
                run_cables[0].log.id
                status += "+"
            except ExecLog.DoesNotExist:
                status += ":"
    st += [status]
#s.RSICs.all()[0].log.all_checks_performed()
#runs[0].run.runsteps.all()[0].RSICs.all()[0].is_complete()
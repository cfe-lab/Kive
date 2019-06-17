from __future__ import unicode_literals

import six

from django.db import transaction


def empty_redaction_plan():
    return {
        "Datasets": set(),
        "ExecRecords": set(),
        "OutputLogs": set(),
        "ErrorLogs": set(),
        "ReturnCodes": set(),
        "ExternalFiles": set()
    }


def summarize_redaction_plan(redaction_plan):
    counts = {key: len(targets) for key, targets in six.iteritems(redaction_plan)}
    return counts


@transaction.atomic
def redact_helper(redaction_plan):
    # Proceed in a fixed order.
    if "Datasets" in redaction_plan:
        for sd in redaction_plan["Datasets"]:
            sd.redact_this()

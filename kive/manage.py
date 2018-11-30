#!/usr/bin/env python
import os
import sys

if __name__ == "__main__":
    subcommand = sys.argv[1] if len(sys.argv) > 1 else None
    default_settings = {'runfleet': 'kive.settings_fleet',
                        'runcontainer': 'kive.settings_fleet_helper',
                        'cable_helper': 'kive.settings_fleet_helper',
                        'step_helper': 'kive.settings_fleet_helper'}
    os.environ["DJANGO_SETTINGS_MODULE"] = (
        default_settings.get(subcommand, "kive.settings"))

    from django.core.management import execute_from_command_line

    execute_from_command_line(sys.argv)

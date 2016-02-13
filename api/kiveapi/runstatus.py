"""
This module defines a class that keeps track
of a run in Kive.
"""
from . import KiveRunFailedException
from .dataset import Dataset


class RunStatus(object):
    """
    This keeps track of a run in Kive.

    There isn't a direct analogue in Kive for this, but it represents a part of
    Run's functionality.
    """

    def __init__(self, obj, api):
        self.run_id = obj['id']
        self.url = obj['run_status']
        self.results_url = obj['run_outputs']
        self.api = api

    def _grab_stats(self):
        data = self.api.get(self.url).json()
        if "!" in data["status"]:
            raise KiveRunFailedException("Run %s failed" % self.run_id)
        return data

    def get_status(self):
        """
        Queries the server for the status of a run

        :return: A description string of the status
        """
        # TODO: Make change kive to return sane overall statuses
        status = self._grab_stats()['status']

        if status == '?':
            return "Waiting to start..."

        if '!' in status:
            raise KiveRunFailedException("Run %s failed" % self.run_id)

        if '*' in status and '.' not in status:
            return 'Complete.'

        return 'Running...'

    def is_waiting(self):
        """
        Returns whether or not the run is queued
        on the server for processing.

        :return:
        """
        status = self._grab_stats()['status']
        return status == '?'

    def is_running(self):
        """
        Returns whether or not the run is running
        on the server

        :return:
        """

        status = self._grab_stats()
        return status.get('start', False) and not status.get('end', False)

    def is_complete(self):
        """
        Returns whether or not the run has
        completed.

        :return:
        """
        status = self._grab_stats()

        return status.get('end', None) is not None

    def is_successful(self):
        """
        Returns whether the run was successful,
        provided that it's also complete

        :return:
        """
        return self.is_complete()

    def get_progress(self):
        """
        Gets the current run's progress bar

        :return:
        """

        return self._grab_stats()['status']

    def get_progress_percent(self):
        """
        Gets the current progress as a percentage.

        :return:
        """
        status = self._grab_stats()['status']
        return 100*float(status.count('*'))/float(len(status) - status.count('-'))

    def get_inputs(self):
        """
        Gets all the datasets that fed this pipeline.

        :return: A list of Dataset objects.
        """
        datasets = self.api.get(self.results_url).json()['input_summary']
        return [Dataset(d, self.api) for d in datasets]

    def get_results(self):
        """
        Gets all the datasets that resulted from this pipeline.

        Includes pipeline outputs and intermediate results. If the run is still
        active, return any outputs that are ready.
        :return: A dictionary of Dataset objects, keyed by name.
        """
        datasets = self.api.get(self.results_url).json()['output_summary']
        return {d['name']: Dataset(d, self.api) for d in datasets}

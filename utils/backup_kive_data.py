#! /usr/bin/env python

import smtplib
from email.mime.text import MIMEText
import json
import subprocess
import os
from datetime import datetime

email_template = """\
Kive backup notification

Kive backup appears to have failed.

Reason: {}

This is an automated e-mail generated by backup_kive_data.py.

{}

{}
"""


def main():
    # Collect the settings from the environment and put them into a dict with keys
    # - smtp_server
    # - email_author (a proper email address)
    # - email_recipients (a list)
    # - kive_data_dir
    # - backup_dir
    # - output_log
    # - error_log
    backup_settings = {
        "smtp_server": os.environ.get("KIVE_BACKUP_SMTP_SERVER", "your.mail.server.example.com"),
        "email_author": os.environ.get("KIVE_BACKUP_EMAIL_AUTHOR", "no.reply.kive.backup@example.com"),
        "email_recipients": json.loads(os.environ.get("KIVE_BACKUP_EMAIL_RECIPIENTS", "[]")),
        "kive_data_dir": os.environ.get("KIVE_BACKUP_SOURCE_DIRECTORY", "./data"),
        "backup_dir": os.environ.get("KIVE_BACKUP_TARGET_DIRECTORY", "./backup"),
        "output_log": os.environ.get("KIVE_BACKUP_OUTPUT_LOG", "./backup_kive_data.stdout"),
        "error_log": os.environ.get("KIVE_BACKUP_ERROR_LOG", "./backup_kive_data.stderr"),
    }

    failure_message = ""

    # Run rsync on all the directories to be backed up.
    data_dirs = ["CodeResources", "Datasets", "Logs", "Containers", "ContainerLogs"]
    cmd_list = [
        "rsync",
        "-a",
        "-h",
        "--delete",
        "[PATH TO BACKUP]",  # dummy value that will be overwritten later
        "[PATH TO BACKUP TO]"  # also a dummy value
    ]

    rsync_stdout_string = ""
    rsync_stderr_string = ""
    # Look for/create a lock file.
    still_locked = False
    lock_file_path = os.path.join(backup_settings["backup_dir"], "rsync_in_progress")
    if os.path.exists(lock_file_path):
        failure_message = "lock file from a previous attempt still exists"
        rsync_stdout_string = "No stdout generated"
        rsync_stderr_string = "No stderr generated"
        still_locked = True
    else:
        with open(lock_file_path, "w"):
            pass  # create a dummy lock file

        rsync_failure = False
        for i, data_dir in enumerate(data_dirs):
            source_dir = os.path.join(backup_settings["kive_data_dir"], data_dir)
            if not os.path.exists(source_dir):
                continue
            cmd_list[4] = source_dir  # this has no trailing slash...
            cmd_list[5] = os.path.normpath(backup_settings["backup_dir"])  # ... so it will use a subdirectory here

            if i == 0:
                file_mode = "wb"
            else:
                file_mode = "ab"
            try:
                with open(backup_settings["output_log"], file_mode) as f:
                    with open(backup_settings["error_log"], file_mode) as g:
                        subprocess.check_call(cmd_list, stdout=f, stderr=g)

            except subprocess.CalledProcessError:
                rsync_failure = True
                failure_message = "rsync failed while backing up {}".format(cmd_list[4])
                break

            finally:
                try:
                    with open(backup_settings["output_log"], "rb") as f:
                        rsync_stdout_string = "stdout log:\n----\n{}\n----".format(f.read())
                except IOError:
                    rsync_stdout_string = "Note: stdout log could not be read"

                try:
                    with open(backup_settings["error_log"], "rb") as f:
                        rsync_stderr_string = "stderr log:\n----\n{}\n----".format(f.read())
                except IOError:
                    rsync_stderr_string = "Note: stderr log could not be read"

        os.remove(lock_file_path)

    # Now, compose and send an email.
    if still_locked or rsync_failure:
        print("[{}] {}".format(datetime.now(), failure_message))
        msg = MIMEText(
            email_template.format(
                failure_message,
                rsync_stdout_string,
                rsync_stderr_string
            )
        )
        msg['Subject'] = 'Kive backup warning'
        msg['From'] = backup_settings["email_author"]
        msg['To'] = backup_settings["email_author"]

        # Do SMTP transaction.
        if len(backup_settings["email_recipients"]) > 0:
            smtp = smtplib.SMTP(backup_settings["smtp_server"])
            smtp.ehlo()
            smtp.starttls()
            smtp.sendmail(backup_settings["email_author"], backup_settings["email_recipients"], msg.as_string())
            smtp.quit()

    else:
        print("[{}] Backup appears to have succeeded.".format(datetime.now()))


if __name__ == "__main__":
    main()

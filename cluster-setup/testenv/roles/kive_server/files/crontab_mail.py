#! /usr/bin/env python3
import logging.config
import smtplib
from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter
from email.message import EmailMessage
from subprocess import run, CalledProcessError, PIPE, STDOUT
from traceback import format_exc


def parse_args():
    # noinspection PyTypeChecker
    parser = ArgumentParser(
        description='Emulate the MAILTO feature of crontab, by mailing output '
                    'from a command.',
        epilog='If the command return code is zero, stdout and stderr are '
               'logged at INFO level, otherwise at ERROR level. The log file '
               'is not opened until after the command completes, so it can be '
               'shared with the command.',
        formatter_class=ArgumentDefaultsHelpFormatter)
    parser.add_argument('--log',
                        help='log file to write info and errors in')
    parser.add_argument('--log_size',
                        default=1000,
                        type=int,
                        help='maximum log size in KB before rolling over')
    parser.add_argument('--log_count',
                        default=3,
                        type=int,
                        help='number of archived log files to keep')
    parser.add_argument('--level',
                        choices=('INFO', 'ERROR'),
                        default='INFO',
                        help='level of messages to email')
    parser.add_argument('--subject',
                        default='Crontab mail',
                        help='e-mail subject line')
    parser.add_argument('--from',
                        default='no-reply@example.com',
                        help='e-mail address to send messages from')
    parser.add_argument('to',
                        help='e-mail addresses to send messages to')
    parser.add_argument('command',
                        nargs='+',
                        help='command and arguments to execute')
    return parser.parse_args()


def main():
    args = parse_args()
    if args.log is None:
        logger = None
    else:
        file_handler = {'class': 'logging.handlers.RotatingFileHandler',
                        'formatter': 'basic',
                        'filename': args.log,
                        'level': 'INFO',
                        'maxBytes': args.log_size*1024,
                        'backupCount': args.log_count,
                        'delay': True}
        logging.config.dictConfig(
            dict(version=1,
                 handlers=dict(file=file_handler),
                 formatters=dict(basic=dict(format='%(asctime)s[%(levelname)s]'
                                                   '%(message)s',
                                            datefmt='%Y-%m-%d %H:%M:%S')),
                 root=dict(handlers=['file'],
                           level='INFO')))
        logger = logging.getLogger()
    # noinspection PyBroadException
    try:
        result = run(args.command, stdout=PIPE, stderr=STDOUT, check=True)
        message_body = result.stdout.decode('utf8').rstrip()
        if logger is not None:
            event_text = 'Completed.'
            if message_body:
                event_text += '\n'
                event_text += message_body
            logger.info(event_text)
        if args.level == 'ERROR':
            return
        message_subject = args.subject
    except CalledProcessError as ex:
        message_body = ex.stdout.decode('utf8').rstrip()
        if logger is not None:
            event_text = 'Failed.'
            if message_body:
                event_text += '\n'
                event_text += message_body
            logger.error(event_text)
        message_subject = args.subject + ' - FAILED'
        if not message_body:
            message_body = 'Command failed: ' + ' '.join(args.command)
    except Exception:
        if logger is not None:
            logger.error('Error.', exc_info=True)
        message_subject = args.subject + ' - ERROR'
        message_body = 'Command failed: ' + ' '.join(args.command) + '\n'
        message_body += format_exc()

    message = EmailMessage()
    message.set_content(message_body)
    message['Subject'] = message_subject
    message['From'] = getattr(args, 'from')  # from is a reserved word
    message['To'] = args.to
    session = smtplib.SMTP('localhost')
    session.send_message(message)


if __name__ == '__main__':
    main()

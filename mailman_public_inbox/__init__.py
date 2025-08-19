# Copyright (C) 2008-2020 by the Free Software Foundation, Inc.
#
# This file is part of GNU Mailman.
#
# GNU Mailman is free software: you can redistribute it and/or modify it under
# the terms of the GNU General Public License as published by the Free
# Software Foundation, either version 3 of the License, or (at your option)
# any later version.
#
# GNU Mailman is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for
# more details.
#
# You should have received a copy of the GNU General Public License along with
# GNU Mailman.  If not, see <https://www.gnu.org/licenses/>.

"""Public-Inbox archiver."""

import logging
import os
import shlex
import subprocess

from mailman.config import config
from mailman.config.config import external_configuration
from mailman.interfaces.archiver import IArchiver, ArchivePolicy
from mailman.interfaces.listmanager import ListDeletingEvent
from public import public
from urllib.parse import urljoin
from zope.event import classhandler
from zope.interface import implementer


log = logging.getLogger('mailman.archiver')


@public
@implementer(IArchiver)
class PublicInbox:
    """Local Public-Inbox archiver."""

    name = 'public_inbox'
    is_enabled = False

    def __init__(self):
        # Read our specific configuration file
        archiver_config = external_configuration(
            config.archiver.public_inbox.configuration)
        self.base_url = archiver_config.get('general', 'base_url')
        self.public_inbox_config = archiver_config.get('general', 'pi_config')
        self.public_inbox_home = archiver_config.get('general', 'pi_home')
        self.public_inbox_path = archiver_config.get('general', 'pi_path')
        self.auto_create = archiver_config.get('general', 'pi_auto_create')
        self.reload_command = archiver_config.get('general', 'pi_reload_command')

        self.pi_config = {}

        if self.auto_create:
            classhandler.handler(ListDeletingEvent, self.list_deleting_handler)

    def _run_command(self, args, **kwargs):
        env = os.environ.copy()
        env['PI_CONFIG'] = self.public_inbox_config
        env['HOME'] = self.public_inbox_home
        env['PATH'] = self.public_inbox_path

        if 'env' in kwargs:
            kwargs['env'].update(env)
        else:
            kwargs['env'] = env

        return subprocess.run(args, capture_output=True, **kwargs)

    def _parse_publicinbox_config(self):
        if self.pi_config:
            return

        proc = self._run_command(['git', 'config', '-z', '-l', '--includes',
                                  '--file', self.public_inbox_config])
        if proc.returncode != 0:
            log.error('Error (%s) when reading public-inbox config: %s',
                      proc.returncode, proc.stderr)

        for cfg in proc.stdout.split(b'\0'):
            try:
                k, v = [i.decode() for i in cfg.split(b'\n', 1)]
            except (ValueError, UnicodeDecodeError):
                continue

            parts = k.split(".")
            if parts[0] != 'publicinbox' or len(parts) != 3:
                continue
            name, conf = parts[1:]
            if name not in self.pi_config:
                self.pi_config[name] = {}
            self.pi_config[name][conf] = v

    def _get_publicinbox_conf(self, mlist):
        self._parse_publicinbox_config()

        for conf in self.pi_config.values():
            if conf.get('address') == mlist.posting_address:
                return conf
            if conf.get('listid') == mlist.list_id:
                return conf

        return {}

    def list_url(self, mlist):
        """See `IArchiver`."""
        conf = self._get_publicinbox_conf(mlist)
        if conf:
            return conf.get('url')
        return None

    def permalink(self, mlist, msg):
        """See `IArchiver`."""
        list_url = self.list_url(mlist)
        msg_id = msg['message-id']
        if msg_id.startswith("<"):
            msg_id = msg_id[1:]
        if msg_id.endswith(">"):
            msg_id = msg_id[:-1]
        if list_url:
            return urljoin(list_url, msg_id + "/")
        return None

    def archive_message(self, mlist, msg):
        """See `IArchiver`."""
        if not self._ensure_list_created(mlist):
            return None

        env = {'ORIGINAL_RECIPIENT': mlist.posting_address}
        url = self.permalink(mlist, msg)

        proc = self._run_command(['public-inbox-mda', '--no-precheck'],
                                 input=msg.as_string(), env=env,
                                 universal_newlines=True)
        if proc.returncode != 0:
            log.error('%s: public-inbox subprocess exited with error(%s): %s',
                      msg['message-id'], proc.returncode, proc.stderr)
        else:
            log.info('%s: Archived with public-inbox at %s',
                     msg['message-id'], url)
        return url

    def _reload_public_inbox(self):
        # Clear cached config regardless of whether a reload command is set
        self.pi_config = {}

        if not self.reload_command:
            return

        args = shlex.split(self.reload_command)
        proc = self._run_command(args)
        if proc.returncode != 0:
            log.error('Error running public-inbox reload command: %s',
                      proc.stderr)

    def _ensure_list_created(self, mlist):
        if not self.auto_create:
            return False

        log.info(f"Attempting to create public-inbox for {mlist.list_name} with {mlist.archive_policy} and {mlist.advertised} and fqdn {mlist.fqdn_listname}. {mlist}")

        if self._get_publicinbox_conf(mlist):
            return True

        log.info("Does not exist yet, proceeding.")

        if mlist.archive_policy != ArchivePolicy.public or not mlist.advertised:
            return False

        log.info(f"Passed pre-check, creating public-inbox.")

        proc = self._run_command(['public-inbox-init', '-V2',
                                  mlist.list_name,
                                  os.path.join(self.public_inbox_home, mlist.list_name),
                                  urljoin(self.base_url, mlist.list_name + "/"),
                                  mlist.fqdn_listname])
        if proc.returncode != 0:
            log.error('Unable to initialise public-inbox archive for list %s: %s',
                      mlist.list_name, proc.stderr)
            return False
        else:
            log.info('Initialised public-inbox archive for list %s',
                     mlist.list_name)

        self._reload_public_inbox()
        return True

    def list_deleting_handler(self, event):
        if not self.is_enabled or not self.auto_create:
            return

        mlist = event.mailing_list
        conf = self._get_publicinbox_conf(mlist)
        if not conf:
            return

        proc = self._run_command(['git', 'config',
                                  '--file',
                                  self.public_inbox_config,
                                  '--remove-section',
                                  'publicinbox.' + mlist.list_name])
        if proc.returncode != 0:
            log.error('Unable to remove public-inbox config for list %s: %s',
                      mlist.list_name, proc.stderr)
        else:
            log.info('Removed public-inbox config for list %s',
                     mlist.list_name)

        self._reload_public_inbox()

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

from mailman.config import config
from mailman.config.config import external_configuration
from mailman.interfaces.archiver import IArchiver
from public import public
from subprocess import PIPE, Popen
from urllib.parse import urljoin
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
        self.public_inbox_config = archiver_config.get('general', 'pi_config')
        self.public_inbox_home = archiver_config.get('general', 'pi_home')
        self.public_inbox_path = archiver_config.get('general', 'pi_path')

        self.pi_config = {}

    def _parse_publicinbox_config(self):
        if self.pi_config:
            return
        env = os.environ.copy()
        env['PATH'] = self.public_inbox_path

        proc = Popen(['git', 'config', '-z', '-l', '--includes',
                      '--file', self.public_inbox_config],
                     stdout=PIPE, stderr=PIPE, env=env)
        stdout, stderr = proc.communicate()
        if proc.returncode != 0:
            log.error('Error (%s) when reading public-inbox config: %s',
                      proc.returncode, stderr)

        for cfg in stdout.split(b'\0'):
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
        env = os.environ.copy()
        env['ORIGINAL_RECIPIENT'] = mlist.posting_address
        env['PI_CONFIG'] = self.public_inbox_config
        env['HOME'] = self.public_inbox_home
        env['PATH'] = self.public_inbox_path
        url = self.permalink(mlist, msg)

        proc = Popen(
            ['public-inbox-mda', '--no-precheck'],
            stdin=PIPE, stdout=PIPE, stderr=PIPE,
            universal_newlines=True, env=env)
        _, stderr = proc.communicate(msg.as_string())
        if proc.returncode != 0:
            log.error('%s: public-inbox subprocess exited with error(%s): %s',
                      msg['message-id'], proc.returncode, stderr)
        else:
            log.info('%s: Archived with public-inbox at %s',
                     msg['message-id'], url)
        return url

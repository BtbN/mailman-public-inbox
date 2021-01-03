========================================
Mailman archiver plugin for Public Inbox
========================================
This module contains a `Mailman`_ archiver plugin which sends emails to
the `Public Inbox`_ archiver.

.. _Mailman: https://www.list.org
.. _Public Inbox: https://public-inbox.org

The source code is available on Github
https://gitlab.com/tohojo/mailman-public-inbox


Assumptions
===========

For this archiver to work, you need a working public-inbox installation that has
been initialised for your mailing list. Basically, you need to have run
``public-inbox-init`` and ``public-inbox-index`` for the list already (see their
man pages for instructions).

The archiver will run ``public-inbox-mda --no-precheck`` for each message, after
first parsing the public-inbox config to find the list ID using either the
posting_address or list_id config options from public-inbox. The archiver will
also generate a permalink for the message based on the 'url' public-inbox config
parameter and the message ID, which will be used by mailman for the Archived-At
header.

Copyright & Licensing
=====================

This module is licensed under the
`GPL v3.0 <http://www.gnu.org/licenses/gpl-3.0.html>`_

Copyright (C) 2021 by Toke Høiland-Jørgensen

#!/usr/bin/python

# This file is part of the statmon project
# Copyright (C) 2007 Jakob Simon-Gaarde <info at skolesys.dk>
#
# This library is free software; you can redistribute it and/or
# modify it under the terms of the GNU Library General Public
# License version 2 as published by the Free Software Foundation.
#
# This library is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# Library General Public License for more details.
#
# You should have received a copy of the GNU Library General Public License
# along with this library; see the file COPYING.LIB.  If not, write to
# the Free Software Foundation, Inc., 51 Franklin Street, Fifth Floor,
# Boston, MA 02110-1301, USA.

from optparse import OptionParser
from statmon_sync import updatedb
from statmon_common import db_truncate
import sys,os

if __name__=='__main__':
	
	shell_cmd_name = os.path.split(sys.argv[0])[-1:][0]
	usage = "usage: %s [options] paths" % shell_cmd_name

	parser = OptionParser(usage=usage)

	parser.add_option("-d", "--db-file", dest="db_file",default="%s/.statmon/fileinfo.db" % os.environ['HOME'],
		help="Path to an sqlite3 db-file to update [default: ~/.statmon/fileinfo.db]", metavar="DB_FILE")
	parser.add_option("-i", "--flush-interval", dest="flush_interval",default=0,
		help="Start inotifier and flush with this interval in seconds [default: 0 - (inotify off)]", metavar="FLUSH_INTERVAL")
	parser.add_option("-e", "--fs-encodings", dest="fs_encodings",default=None,
		help="A prioritized list of encodings to try on the monitored paths", metavar="FS_ENCODINGS")
	parser.add_option("-v", "--verbose", action="store_true", dest="verbose", default=False,
		help="Print debug information to screen")
	parser.add_option("-t", "--truncate", action="store_true", dest="truncate", default=False,
		help="Truncate the fileinfo database")
	(options, args) = parser.parse_args()

	if not os.path.exists("%s/.statmon" % os.environ['HOME']):
		os.makedirs("%s/.statmon" % os.environ['HOME'])

	if len(args)<1:
		print parser.usage
		sys.exit(1)
	
	paths = args[0]

	if options.truncate:
		print "TRUNCATING FILE STAT INFO"
		db_truncate(options.db_file)

	print "SYNCHRONIZING DB WITH MONITORED DIRECTORIES"
	updatedb(paths.split(':'),options.db_file,options.fs_encodings.split(','))

	flush_interval = 0
	try:
		flush_interval = int(options.flush_interval)
	except:
		pass
	
	if flush_interval:
		from statmon_inotify import get_notifier
		print "STARTING FILE STAT MONITOR"
		handler,notifier = get_notifier(paths.split(':'),options.db_file,flush_interval,options.verbose)

		while True:  # loop forever
			try:
				# process the queue of events as explained above
				notifier.process_events()
				if notifier.check_events():
					# read notified events and enqeue them
					notifier.read_events()
				# you can do some tasks here...
			except KeyboardInterrupt:
				# destroy the inotify's instance on this interrupt (stop monitoring)
				notifier.stop()
				handler.timer.cancel()
				break
	
	
#	# Example on how to create a threaded notifier that monitors stat-changes using inotify
#	print	
#	print "STARTING FILE STAT MONITOR"
#	handler,notifier = get_notifier(monitor_paths.split(':'),db_file,update_interval,verbose)
#	
#	try:
#		time.sleep(1000)
#	except KeyboardInterrupt:
#		print "  Program termination signal recieved"
#	
#	notifier.stop()
#	handler.timer.cancel()

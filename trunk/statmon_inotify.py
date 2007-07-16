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

import pysqlite2.dbapi2 as pysqlite
import sys,os,threading,sys
from statmon_common import md5_reduce,log_error
from statmon_sync import sync_db_remove_deleted_files,sync_db_update_missing_files
from pyinotify import WatchManager, Notifier, ThreadedNotifier, EventsCodes, ProcessEvent


mask = EventsCodes.IN_DELETE 
mask |= EventsCodes.IN_CREATE
mask |= EventsCodes.IN_CLOSE_WRITE
mask |= EventsCodes.IN_ATTRIB
mask |= EventsCodes.IN_MOVED_FROM
mask |= EventsCodes.IN_MOVED_TO
mask |= EventsCodes.IN_IGNORED

class FileStatEventHandler(ProcessEvent):
	
	def __init__(self,wm,db_file,flush_timeout=5,verbose=False):
		self.wm = wm
		self.db_file = db_file
		self.verbose = verbose
		self.updatebuffer = {}
		self.eventcounter = 0
		self.thread_lock = threading.Lock()
		self.flush_timeout = flush_timeout
		self.timer = threading.Timer(flush_timeout,self.timeout,[])
		self.timer.start()
		self.new_dirs = {}
	
		
	
	def flush(self):
		global mask
		if self.thread_lock.acquire(0) == False:
			return
	
		updatelist = []
		deletelist = []
		rec_deletelist = []
		
		for k,v in self.updatebuffer.items():
			del self.updatebuffer[k]
			directory,basename = os.path.split(k)
			if v=='u':
				try:
					stat = os.stat(k)
				except OSError, e:
					# File or directory does not exist
					log_error(e)
					continue
				
				updatelist += [{
					'path' : k,
					'basename' : basename,
					'directory' : directory,
					'uid' : stat.st_uid,
					'gid' : stat.st_gid,
					'size' : stat.st_size,
					'md5sum' : md5_reduce(k,stat)}]
			elif v=='d':
				deletelist += [{'path':k}]
			elif v=='dr':
				rec_deletelist += [{'path_plus': '%s/%%' % k,'path':k}]
				
		delete_rows = len(deletelist)
		rec_delete_rows = len(rec_deletelist)
		update_rows = len(updatelist)
		changes = 0
		
		if delete_rows or update_rows or rec_delete_rows:
			# Connect to DB
			print "Updating",
			sys.stdout.flush()
			con = pysqlite.connect(self.db_file)
			
			if update_rows:
				con.executemany("""
					insert or replace into fileinfo (
						path,basename,directory,uid,gid,size,md5sum) 
					select
						:path,:basename,:directory,:uid,:gid,:size,:md5sum
					where not exists (
						select 1
						from fileinfo
						where md5sum=:md5sum)""", updatelist)
				# Now get the real amount of affected rows
				update_rows = con.total_changes
				
			if delete_rows:
				con.executemany("delete from fileinfo where path=:path",deletelist)
				# Now get the real amount of affected rows
				delete_rows = con.total_changes - update_rows
			
			if rec_delete_rows:
				con.executemany("delete from fileinfo where path like :path_plus or path=:path", rec_deletelist)
				delete_rows = con.total_changes - update_rows
	
			con.commit()
			con.close()
			del con
		
		for path,last_sync_count in self.new_dirs.items():
			this_sync_count = sync_db_update_missing_files(self.db_file,path)
			if this_sync_count==0: # and last_sync_count==0: (Uncomment to use doublecheck)
				del self.new_dirs[path]
				wdd = self.wm.add_watch(path, mask, rec=True,auto_add=False)
			else:
				self.new_dirs[path] = this_sync_count
			update_rows += this_sync_count
		
		self.eventcounter = 0
		self.thread_lock.release()

		if update_rows or delete_rows:
			print "- update_rows: %d, delete_rows: %d" % (update_rows,delete_rows)
			sys.stdout.flush()

		return True
		
	def timeout(self):
		self.flush()
		self.timer = threading.Timer(self.flush_timeout,self.timeout,[])
		self.timer.start()

		
	def maybeFlush(self):
		if self.eventcounter<500:
			return
		self.flush()

	def register_change(self,action, dirname, names):
		if type(names) == str:
			names = [names]
		for n in names:
			self.updatebuffer[os.path.normpath(os.path.join(dirname,n))] = action
			self.eventcounter += 1
			self.maybeFlush()
	
	
	def process_IN_CREATE(self, event):
		path = os.path.normpath(os.path.join(event.path,event.name))
		if event.is_dir and self.wm.get_wd(path)==None:
			self.new_dirs[path] = 1
		if self.verbose:
			print "CREATE: %s" % path
		self.register_change('u',event.path,event.name)

	def process_IN_DELETE(self, event):
		if self.verbose:
			print "DELETE: %s" % os.path.normpath(os.path.join(event.path,event.name))
		self.register_change('d',event.path,event.name)
	
	def process_IN_CLOSE_WRITE(self, event):
		if self.verbose:
			print "WRITE: %s" % os.path.normpath(os.path.join(event.path,event.name))
		self.register_change('u',event.path,event.name)

	def process_IN_ATTRIB(self, event):
		if self.verbose:
			print "ATTRIB: %s" % os.path.normpath(os.path.join(event.path,event.name))
		self.register_change('u',event.path,event.name)

	def process_IN_MOVED_TO(self, event):
		if self.verbose:
			print "MOVED_TO: %s" % os.path.normpath(os.path.join(event.path,event.name))
		if event.is_dir:
			# Register all subdirectories
			os.path.walk(os.path.normpath(os.path.join(event.path,event.name)), self.register_change,'u')
		# Register and the subject itself
		self.register_change('u',event.path,event.name)

	def process_IN_MOVED_FROM(self, event):
		if self.verbose:
			print "MOVED_FROM: %s" % os.path.normpath(os.path.join(event.path,event.name))
		if event.is_dir:
			# We only need to register the path and set the 'r'-recursive flag
			self.register_change('dr',event.path,event.name)
		else:
			self.register_change('d',event.path,event.name)

	#def process_IN_MOVE_SELF(self, event):
		#if self.verbose:
			#print "MOVE_SELF: %s" % os.path.normpath(os.path.join(event.path,event.name))
		#if event.is_dir:
			## We only need to register the path and set the 'r'-recursive flag
			#self.register_change('dr',event.path,event.name)

	#def process_IN_DELETE_SELF(self, event):
		#if self.verbose:
			#print "DELETE_SELF: %s" % os.path.normpath(os.path.join(event.path,event.name))
		#if event.is_dir and event.wd==1:
			## We only need to register the path and set the 'r'-recursive flag
			#self.register_change('dr',event.path,event.name)

	def process_IN_IGNORED(self, event):
		pass



def get_threaded_notifier(paths,db_file,flush_interval,verbose):

	wm = WatchManager()
	eventhandler = FileStatEventHandler(wm,db_file,flush_interval,verbose)
	
	notifier = ThreadedNotifier(wm, eventhandler)
	notifier.start()
	for p in paths:
		if verbose:
			print '  Monitoring "%s" ... ' % p,
		wdd = wm.add_watch(p, mask, rec=True,auto_add=False)
		if verbose:
			print '[done]'
	
	return eventhandler,notifier

def get_notifier(paths,db_file,flush_interval,verbose):
	wm = WatchManager()
	handler = FileStatEventHandler(wm,db_file,flush_interval,verbose)
	notifier = Notifier(wm, handler)
	
	for p in paths:
		if verbose:
			print '  Monitoring "%s" ... ' % p,
		wdd = wm.add_watch(p, mask, rec=True,auto_add=False)
		if verbose:
			print '[done]'
	return handler,notifier


#	# Example on how to create a threaded notifier that monitors stat-changes using inotify
#	print	
#	print "STARTING FILE STAT MONITOR"
#	handler,notifier = get_threaded_notifier(monitor_paths.split(':'),db_file,update_interval,verbose)
#	
#	try:
#		time.sleep(1000)
#	except KeyboardInterrupt:
#		print "  Program termination signal recieved"
#	
#	notifier.stop()
#	handler.timer.cancel()

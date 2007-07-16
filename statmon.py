import pysqlite2.dbapi2 as pysqlite

from pyinotify import WatchManager, Notifier, ThreadedNotifier, EventsCodes, ProcessEvent
import os,threading,md5,sys,pprint,time

def log_error(error):
	f = open('error.log','a')
	f.write("%s\n" % error)
	f.close()


def md5_reduce(path,stat):
	return md5.new('%s:%d:%d:%d' % (path,stat.st_uid,stat.st_gid,stat.st_size)).hexdigest()


wm = WatchManager()

mask = EventsCodes.IN_DELETE 
mask |= EventsCodes.IN_CREATE
mask |= EventsCodes.IN_CLOSE_WRITE
mask |= EventsCodes.IN_ATTRIB
mask |= EventsCodes.IN_MOVED_FROM
mask |= EventsCodes.IN_MOVED_TO
#mask |= EventsCodes.IN_MOVE_SELF
#mask |= EventsCodes.IN_DELETE_SELF
mask |= EventsCodes.IN_IGNORED

class FileStatEventHandler(ProcessEvent):
	
	def __init__(self,db_file,flush_timeout=5,verbose=False):
		self.db_file = db_file
		self.verbose = verbose
		self.updatebuffer = {}
		self.eventcounter = 0
		self.thread_lock = threading.Lock()
		self.flush_timeout = flush_timeout
		self.timer = threading.Timer(flush_timeout,self.timeout,[])
		self.timer.start()
		self.new_dirs = {}
	
		
	def sync_db_remove_deleted_files(self,fs_encoding=None):
		con = pysqlite.connect(self.db_file)
		
		cur = con.cursor()
		deletelist = []

		print "  Checking for deleted files ... ",
		cur.execute('select path from fileinfo')
		res = cur.fetchmany(1000)
		while res:
			for row in res:
				path = row[0]
				if fs_encoding:
					fs_path = path.encode(fs_encoding)
				else:
					fs_path = path
				if not os.path.exists(fs_path):
					deletelist += [{'path':row[0]}]
			res = cur.fetchmany(1000)
		print "[done]"
	
		del cur
		
		delete_rows = len(deletelist)
		
		if delete_rows:
			print "   * Deleting %d row(s)" % delete_rows
			con.executemany("delete from fileinfo where path=:path",deletelist)
		
			con.commit()
			print
	
		con.close()
		del con
		return delete_rows

	
	def sync_db_update_missing_files(self,directory,verbose=False,fs_encoding=None):
		con = pysqlite.connect(self.db_file)
		
		cur = con.cursor()
		updatelist = []
		
		def stat(args, dirname, names):
			cur,updatelist,fs_encoding = args
			# Create unicode string if fs_encoding is set
			if fs_encoding:
				db_dirname = dirname.decode(fs_encoding)
			else:
				db_dirname = dirname
			for n in names:
				try:
					if fs_encoding:
						db_name = n.decode(fs_encoding)
					else:
						db_name = n
					db_path = os.path.join(db_dirname,db_name)
					path = os.path.join(dirname,n)
					stat = os.stat(path)
					args = {'md5sum': md5_reduce(path,stat)}
					cur.execute('select 1 from fileinfo where md5sum=:md5sum',args)
					if not cur.fetchone():
						updatelist += [{
							'path' : db_path,
							'basename' : db_name,
							'directory' : db_dirname,
							'uid' : stat.st_uid,
							'gid' : stat.st_gid,
							'size' : stat.st_size,
							'md5sum' : args['md5sum']}]
					
				except Exception, e:
					log_error(e)
					pass
					
		# Pre-sync DB with filesystem
		if verbose:
			print "  Checking for new files and file alterations %s ... " % directory,
		sys.stdout.flush()
		os.path.walk(directory,stat,(cur,updatelist,fs_encoding))
		if verbose:
			print "[done]"
		
		del cur
		
		update_rows = len(updatelist)
		if update_rows:
			if verbose:
				print "  Updating file stat information db:"
				print "   * Adding or updating %d row(s)" % update_rows
				print
			if update_rows:
				con.executemany("""
					insert or replace into fileinfo (
						path,basename,directory,uid,gid,size,md5sum) values (
						:path,:basename,:directory,:uid,:gid,:size,:md5sum)""", updatelist)
			
			con.commit()
	
		con.close()
		del con
		return update_rows

	
	def flush(self):
		global wm,mask
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
			this_sync_count = self.sync_db_update_missing_files(path)
			if this_sync_count==0: # and last_sync_count==0: (Uncomment to use doublecheck)
				print "if: ",self.new_dirs
				del self.new_dirs[path]
				wdd = wm.add_watch(path, mask, rec=True,auto_add=False)
				print wdd
			else:
				print "else: ",self.new_dirs
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
		global wm
		path = os.path.normpath(os.path.join(event.path,event.name))
		if event.is_dir and wm.get_wd(path)==None:
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

if __name__=='__main__':
	
	print
	monitor_paths = '/skolesys/denskaegge.dk'
	#monitor_paths = '/skolesys/denskaegge.dk/users/jakob/.linux'
	db_file = '/home/admin/statmon/fileinfo'
	update_interval = 5
	verbose = False
	fs_encoding = 'iso-8859-1'
		
	print "SYNCHRONIZING DB WITH MONITORED DIRECTORIES"
	# Create inotifier
	eventhandler = FileStatEventHandler(db_file,update_interval,verbose)
	eventhandler.sync_db_remove_deleted_files(fs_encoding=fs_encoding)
	for p in monitor_paths.split(':'):
		eventhandler.sync_db_update_missing_files(p,True,fs_encoding=fs_encoding)

	print
	print "STARTING FILE STAT MONITOR"

	#notifier = ThreadedNotifier(wm, eventhandler)
	notifier = Notifier(wm, eventhandler)
	#notifier.start()
	for p in monitor_paths.split(':'):
		print '  Monitoring "%s" ... ' % p,
		wdd = wm.add_watch(p, mask, rec=True,auto_add=False)
		f = open('mon.txt','w')
		pprint.pprint(wdd,f)
		f.write('\n')
		f.close()
		print '[done]'
	print
	
	#try:
	#	time.sleep(1000)
	#except KeyboardInterrupt:
	#	print "  Program termination signal recieved"
	
	#notifier.stop()
	#eventhandler.timer.cancel()
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
			eventhandler.timer.cancel()
			break
	
	

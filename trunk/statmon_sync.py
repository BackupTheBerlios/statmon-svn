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
import os,sys
from statmon_common import md5_reduce,db_check,log_error

def sync_db_remove_deleted_files(db_file,fs_encoding=None):
	con = pysqlite.connect(db_file)
	
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


def sync_db_update_missing_files(db_file,directory,verbose=False,fs_encoding=None):
	con = pysqlite.connect(db_file)
	
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


def updatedb(paths,db_file,fs_encoding=None):
	db_check(db_file)
	sync_db_remove_deleted_files(db_file,fs_encoding=fs_encoding)
	for p in paths.split(':'):
		sync_db_update_missing_files(db_file,p,True,fs_encoding=fs_encoding)

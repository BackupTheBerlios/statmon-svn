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
import os,sys,md5
from statmon_common import md5_reduce,db_check,log_error,try_decode

def sync_db_remove_deleted_files(db_file):
	con = pysqlite.connect(db_file)
	
	cur = con.cursor()
	file_deletelist = []
	dir_deletelist = []

	print "  Checking for deleted files and directories ... ",

	# Check Files
	cur.execute("""
		select 
			d.path,d.encoding as denc,d.md5sum as dir_md5sum,
			f.name,f.encoding as fenc,f.md5sum as file_md5sum 
		from file f,directory d 
		where f.dir_md5sum=d.md5sum""")
	res = cur.fetchmany(1000)
	while res:
		for row in res:
			if row[1]:
				directory = row[0].encode(row[1])
			else:
				directory = row[0]
			
			if row[4]:
				name = row[3].encode(row[4])
			else:
				name = row[3]
			path = os.path.join(directory,name)
			if not os.path.exists(path):
				file_deletelist += [{'dir_md5sum':row[2], 'md5sum': row[5]}]
		res = cur.fetchmany(1000)

	# Check Directories	
	cur.execute("select d.path,d.encoding,d.md5sum from directory d")
	res = cur.fetchmany(1000)
	while res:
		for row in res:
			if row[1]:
				directory = row[0].encode(row[1])
			else:
				directory = row[0]
			if not os.path.exists(directory):
				dir_deletelist += [{'md5sum': row[2]}]
		res = cur.fetchmany(1000)

	del cur

	print "[done]"
	
	delete_file_rows = len(file_deletelist)
	if delete_file_rows:
		print "   * Files: Deleting %d row(s)" % delete_file_rows
		con.executemany("delete from file where dir_md5sum=:dir_md5sum and md5sum=:md5sum",file_deletelist)
	
		con.commit()
		print
	
	delete_dir_rows = len(dir_deletelist)
	
	if delete_dir_rows:
		print "   * Directories: Deleting %d row(s)" % delete_dir_rows
		con.executemany("delete from directory where md5sum=:md5sum",dir_deletelist)
	
		con.commit()
		print

	con.close()
	del con
	return delete_file_rows + delete_dir_rows


def sync_db_update_missing_files(db_file,directory,verbose=False,fs_encodings=[]):
	con = pysqlite.connect(db_file)
	
	cur = con.cursor()
	file_updatelist = []
	dir_updatelist = []
	
	def stat(args, dirname, names):
		cur,dirs,files,fs_encodings = args
		# Create unicode string if fs_encoding is set
		
		u_dirname,enc = try_decode(dirname,fs_encodings)
		
		stat = os.stat(dirname)
		dir_md5sum = md5_reduce(dirname,stat)
		args = {'md5sum': dir_md5sum}
		cur.execute('select 1 from directory where md5sum=:md5sum',args)
		if not cur.fetchone():
			dirs += [{
				'path': u_dirname,
				'uid' : stat.st_uid,
				'gid' : stat.st_gid,
				'md5sum' : dir_md5sum,
				'encoding' : enc}]
		
		for n in names:
			try:
				u_name,enc = try_decode(n,fs_encodings)
				path = os.path.join(dirname,n)
				stat = os.stat(path)
				args = {'md5sum': md5_reduce(path,stat)}
				cur.execute('select 1 from file where md5sum=:md5sum',args)
				if not cur.fetchone():
					files += [{
						'dir_md5sum' : dir_md5sum,
						'name' : u_name,
						'uid' : stat.st_uid,
						'gid' : stat.st_gid,
						'size' : stat.st_size,
						'encoding': enc,
						'md5sum' : args['md5sum']},]
				
			except Exception, e:
				log_error(e)
				pass
				
	# Pre-sync DB with filesystem
	if verbose:
		print "  Checking for new files and file alterations %s ... " % directory,
	sys.stdout.flush()
	os.path.walk(directory,stat,(cur,dir_updatelist,file_updatelist,fs_encodings))
	if verbose:
		print "[done]"
	
	del cur
	
	update_rows = len(dir_updatelist)
	if update_rows:
		if verbose:
			print "  Updating directory information db:"
			print "   * Adding or updating %d row(s)" % update_rows
			print
		con.executemany("""
			insert or replace into directory (
				path,uid,gid,encoding,md5sum) values (
				:path,:uid,:gid,:encoding,:md5sum)""", dir_updatelist)
		
		con.commit()

	update_rows = len(file_updatelist)	
	if update_rows:
		if verbose:
			print "  Updating file information db:"
			print "   * Adding or updating %d row(s)" % update_rows
			print
		con.executemany("""
			insert or replace into file (
				dir_md5sum,name,uid,gid,size,encoding,md5sum) values (
				:dir_md5sum,:name,:uid,:gid,:size,:encoding,:md5sum)""", file_updatelist)
		
		con.commit()

	con.close()
	del con
	return update_rows


def updatedb(paths,db_file,fs_encodings=[],verbose=False):
	db_check(db_file)
	sync_db_remove_deleted_files(db_file)
	for p in paths:
		sync_db_update_missing_files(db_file,p,verbose,fs_encodings=fs_encodings)

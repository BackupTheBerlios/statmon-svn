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
import md5


def db_check(db_file):
	con = pysqlite.connect(db_file)
	cur = con.cursor()
	cur.execute('pragma table_info(directory)')
	res = cur.fetchone()
	if not res or not len(cur.fetchone()):
		con.execute("CREATE TABLE directory (path text, uid integer, gid integer,encoding text,md5sum text)")
		con.execute("CREATE UNIQUE INDEX idx_directory_dir_id on directory(md5sum)")
		con.commit()
		
	cur.execute('pragma table_info(file)')
	res = cur.fetchone()
	if not res or not len(cur.fetchone()):
		con.execute("CREATE TABLE file (dir_md5sum text, name text, uid integer, gid integer,size numeric, md5sum text,encoding text)")
		con.execute("CREATE INDEX idx_file_dir_md5sum on file(dir_md5sum)")
		con.execute("CREATE UNIQUE INDEX idx_file_md5sum on file(md5sum)")
		con.commit()
	del cur
	con.close()


def db_truncate(db_file):
	con = pysqlite.connect(db_file)
	con.execute("delete from file;")
	con.execute("delete from directory;")
	con.commit()
	con.close()


def log_error(error):
	f = open('error.log','a')
	f.write("%s\n" % error)
	f.close()


def try_decode(subject,encodings):
	if len(encodings) == 0:
		return subject,None
	unicode_subject = None
	for enc in encodings:
		try:
			unicode_subject = subject.decode(enc)
			break
		except:
			pass
	if unicode_subject == None:
		log_error("Failed to decode string")
		return None,None
	
	return unicode_subject, enc


def md5_reduce(path,stat):
	return md5.new('%s:%d:%d:%d' % (path,stat.st_uid,stat.st_gid,stat.st_size)).hexdigest()

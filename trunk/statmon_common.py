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
	cur.execute('pragma table_info(fileinfo)')
	res = cur.fetchone()
	if not res or not len(cur.fetchone()):
		con.execute("CREATE TABLE fileinfo (path text, basename text, directory text, uid integer, gid integer,size numeric, md5sum text)")
		con.execute("CREATE UNIQUE INDEX idx_fileinfo_path on fileinfo(path)")
		con.execute("CREATE INDEX idx_fileinfo_md5sum on fileinfo(md5sum)")
		con.commit()
	del cur
	con.close()

def log_error(error):
	f = open('error.log','a')
	f.write("%s\n" % error)
	f.close()


def md5_reduce(path,stat):
	return md5.new('%s:%d:%d:%d' % (path,stat.st_uid,stat.st_gid,stat.st_size)).hexdigest()

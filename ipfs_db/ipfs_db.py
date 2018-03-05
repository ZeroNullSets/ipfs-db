#!/bin/env python3
import sys
try:
    import sqlite3 as sql
except ImportError:
    print("You need an sqlite3 module to run this program!")
import time
try:
    import ipfsapi
except ImportError:
    print("You need an ipfsapi module to run this program!")
    print("pip install ipfsapi")
    raise SystemExit
class IPFSDB():
    def __init__(self, _db="ipfs.db"):
        self.ipfs = ipfsapi.connect()
        self.conn = sql.connect(_db)
        self.c = self.conn.cursor()
        self.create_db()

    def create_db(self):

        self.c.execute('''CREATE TABLE IF NOT EXISTS files(
                                                    hash text primary key,
                                                    filename text not null,
                                                    add_time integer not null)
                                                    ''')
        self.c.execute('''CREATE TABLE IF NOT EXISTS tags(
                                                    tag_name text primary key) 
                                                    ''')
        self.c.execute('''CREATE TABLE IF NOT EXISTS files_fk_tags(
                                                            file_hash text, 
                                                            tag_name text, 
                                                            FOREIGN KEY(file_hash) REFERENCES files(hash), 
                                                            FOREIGN KEY(tag_name) REFERENCES tags(tag_name))
                                                            ''')

    def find_files_by_tag(self, tag):
        c_ex = self.c.execute("select files.hash from files inner join files_fk_tags on files.hash=files_fk_tags.file_hash inner join tags on files_fk_tags.tag_name=tags.tag_name where tags.tag_name=? group by files.hash", (tag,))
        files_fetch = c_ex.fetchall()
        files = []
        for file in files_fetch:
            files += file
        return files

    def add_hash(self, _filename, _hash, tags=[]):
        filename = str(_filename)
        hash = str(_hash)
        try:
            c_ex = self.c.execute("insert into files(filename, hash, add_time) values(?,?,?)",(filename, hash, int(time.time())))
        except sql.IntegrityError:
            pass
        except sql.DatabaseError:
            print("Cannot add file to database!\nExiting...");
            raise SystemExit
        if len(tags) == 0:
            self.conn.commit()
        else:
            for tag in tags:
                try:
                    print("Adding tag:",tag)
                    c_ex = self.c.execute("insert into tags(tag_name) values(?)", (tag,))
                    self.conn.commit()
                except sql.IntegrityError:
                    pass
                except sql.DatabaseError:
                    print("Cannot add tag to database!\nExiting...")
                    raise SystemExit
                print("Adding tag", tag, "to hash", hash)
                c_ex = self.c.execute("select file_hash from files_fk_tags where file_hash=? and tag_name=?", (hash, tag))
                if len(c_ex.fetchall()) != 0:
                    continue
                try:
                    c_ex = self.c.execute("insert into files_fk_tags values(?,?)",(hash, tag))
                except sql.IntegrityError:
                    pass
                self.conn.commit()
    def add_file(self, path, tags=[]):
        hash = self.ipfs.add(str(path))
        self.add_hash(hash['Name'], hash['Hash'], tags)
        return hash['Hash']

    #def get_tag_id(self, tag):
    #    c_ex = c.execute("select id from tags where tag_name=?",(str(tag),))
    #    print(c_ex)
    #
    #    id = c_ex.fetchall()
    #    print(id)
    #    return id;

if __name__ == "__main__":
    args = sys.argv[1:]
    db = IPFSDB()
    if len(args) == 0:
        print("Usage:")
        print("If you like to add file to IPFS:\npython ipfs-db.py add path/to/file tag1 [...]")
        print("If you like to find hashes with tags:\npython ipfs-db.py find tag1 [...]")
        print("You can also add hash, download file from IPFS and add tags to it\npython ipfs-db.py add hash hash tag1 [...]")
        raise SystemExit
    if args[0] == "find":
        args.pop(0)
        for tag in args:
            print("Files with \""+str(tag)+"\" tag:")
            for file in db.find_files_by_tag(tag):
                print(file)
    if args[0] == "add":
        args.pop(0)
        if args[0] == "hash":
            args.pop(0)
            hash = args[0]
            args.pop(0)
            db.ipfs.get(hash)
            db.add_file(hash, args)
        else:
            filename = args[0]
            args.pop(0)
            db.add_file(filename, args) 

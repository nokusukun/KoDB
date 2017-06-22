# Ko-DB a zip and yaml based database
# Author http://me.noku.space
import zipfile
import warnings
import yaml
import uuid
import os
import glob

try:
    import ujson as json
    warnings.warn("KoDB is running on ujson.")
except:
    import json
    pass

KEYS = []
KEYS.extend(range(48, 57))
KEYS.extend(range(65, 90))
KEYS.extend(range(97, 122))


class KoDB():

    def __init__(self, file, **OPTIONS):
        """KoDB(String:database_file, **options)

            Creates a new database object, 3 files will be geenrated
                upon creation.
            Options:
                load_to_memory=Bool:False
                    > Loads the default table into memory
                no_commit=Bool:False
                    > Prevents commit upon storing data
                        (useful for large insert ops)
                load_type=String:safe_load
                    > Specifies what load function yaml uses.
                        (use 'load' for object serialization 
                            support)
                data_suffix=String:default
                    > Specifies the default data suffix.
                        (this is the table seperator)
                """
        
        self.KO_FOLDER = file
        self.KO_FILENAME = "{}.db".format(file)
        self.KO_DB_FILEPATH = os.path.join(self.KO_FOLDER, self.KO_FILENAME)
        self.KO_COMMIT_CACHE = []
        self.KO_META_COMMIT_CACHE = {}
        self.KO_NO_COMMIT = OPTIONS["no_commit"] if "no_commit" in OPTIONS else False
        self.KO_LOADTYPE = OPTIONS["load_type"] if "load_type" in OPTIONS else "safe_load"
        self.KO_DATA_SUFFIX = OPTIONS["data_suffix"] if "data_suffix" in OPTIONS else "default"
        self.KO_LAST_CHUNK_SIZE = 0

        # Check if the database exists
        # Create if not
        # TODO: PUT ALL OF THE NEW_DB CODE HERE
        self.KO_META = []
        if not os.path.exists(self.KO_FOLDER):
            # NEW DB 
            os.mkdir(self.KO_FOLDER)
            os.mkdir(os.path.join(self.KO_FOLDER, "meta"))
            self.KO_META.append({self.KO_DATA_SUFFIX: {}})
            self.KO_CONFIG = {}
            self.ko_set_config("tables", [self.KO_DATA_SUFFIX])
            metasize = 1028 if "meta_size" not in OPTIONS else (OPTIONS["meta_size"])
            self.ko_set_config("metasize", metasize)

        else:
            # LOAD DB
            self.init_meta()
            config_path = os.path.join(self.KO_FOLDER, "{}.KO_CONFIG".format(self.KO_FOLDER))
            with open(config_path) as f:
                self.KO_CONFIG = yaml.load(f.read())

        # Open Database File
        try:
            self.KO_ZIP = zipfile.ZipFile(self.KO_DB_FILEPATH, mode="a", allowZip64=True)
        except:
            self.KO_ZIP = zipfile.ZipFile(self.KO_DB_FILEPATH, mode="x", allowZip64=True)

        self.KO_INDEX = {}
        if "load_to_memory" in OPTIONS:
            if OPTIONS["load_to_memory"]:
                self.load_all_to_memory()

        # data_suffix = self.KO_DATA_SUFFIX if data_suffix is None else data_suffix
        # sets the function to the default table if none is specified


    def close(self):
        """close()

            Closes the database.
            NOTE: THIS DOES NOT COMMIT THE DATABASE"""
        self.KO_ZIP.close()


    def table(self, table_name):
        """table(String:table_name)

            Returns a KO_Table Object. Documents Stored in this context
                is not visible in the database object as it is it's 
                own table."""
        # Appends a table in the table list
        tables = self.ko_get_config("tables")
        if table_name not in tables:
            tables.append(table_name)
            self.ko_set_config("tables", tables)

        return Ko_Table(table_name, self)


    def ko_set_config(self, config_name, value):
        self.KO_CONFIG[config_name] = value
        # The zipfile will throw a bloody warning.
        config_path = os.path.join(self.KO_FOLDER, "{}.KO_CONFIG".format(self.KO_FOLDER))
        with open(config_path, "w") as f:
            f.write(yaml.dump(self.KO_CONFIG))


    def ko_get_config(self, config_name):
        tmp = self.KO_CONFIG
        if config_name in tmp:
            return tmp[config_name]

        return None 


    def load_to_memory(self, data_suffix = None):
        """load_to_memory(String:data_suffix = None)

            Loads all of the database entries into memory.
            Options:
                data_suffix = String:None
                    > The data suffix (table) of the database to load
                        (Loads the default database if unspecified)
                        (***NOTE: This option is usually handled 
                                by the KO_Table Object.***)"""
        data_suffix = self.KO_DATA_SUFFIX if data_suffix is None else data_suffix
        for index in self.items(data_suffix):
            if index in self.KO_INDEX[data_suffix]:
                self.KO_INDEX[data_suffix][index] = self.get(index, data_suffix)


    def get_all(self, data_suffix = None):
        """get_all(String:data_suffix = None)

            Returns a generator iterating through all of the table's
                database entries.
            Options:
                data_suffix = String:None
                    > The data suffix (table) of the database to retrieve
                        (Retrieves the default database if unspecified)
                        (***NOTE: This option is usually handled 
                                by the KO_Table Object.***)"""
        for index in self.items(data_suffix):
            yield self.get(index, data_suffix)


    def exists(self, fid, data_suffix = None):
        """exists(String:fid, String:data_suffix = None)

            Returns True if a database entry exists.
            Options:
                fid = String:Required
                    > The id of the document to check
                data_suffix = String:None
                    > The data suffix (table) on where to check.
                        (Retrieves the default database if unspecified)
                        (***NOTE: This option is usually handled 
                                by the KO_Table Object.***)"""
        try:
            data_suffix = self.KO_DATA_SUFFIX if data_suffix is None else data_suffix
            # if fid in self.KO_META[data_suffix]:
            if self.get_meta(fid, data_suffix) is not None:
                return True
            else:
                return False
            # info = self.KO_ZIP.getinfo("{}.{}".format(fid, data_suffix))
            # return True
        except KeyError:
            return False
            

    def query(self, search_func, data_suffix = None):
        """query(Function:search_function, String:data_suffix = None)

            Returns items that matches the search_function.
            ex.: db.query(lambda x: x.views > 100)
                > Returns items that has views greater than 100
            Options:
                search_func = Lambda/Function:required
                    > The statement that filters the correct data.
                data_suffix = String:None
                    > The table to be queried.
                        (Retrieves the default database if unspecified)
                        (***NOTE: This option is usually handled 
                                by the KO_Table Object.***)"""
        return [x for x in self.get_all(data_suffix) if search_func(x)]


    def store(self, pid, data, data_suffix = None):
        """store(String:pid, Dict:data, String:data_suffix = None)

            Stores a document in the database
            Options:
                pid = String:required
                    > The document ID 
                data = Dictionary:required
                    > The dictionary to be converted into yaml and stored in 
                        the database.
                data_suffix = String:None
                    > The table where the document should be stored.
                        (Stores on the default database if unspecified)
                        (***NOTE: This option is usually handled 
                                by the KO_Table Object.***)"""
        # try:
        data_suffix = self.KO_DATA_SUFFIX if data_suffix is None else data_suffix
        # if data_suffix not in self.KO_META:
        #     self.KO_META[data_suffix] = {}

        uid = str(uuid.uuid4())
        # if pid in self.KO_META[data_suffix]:
        #     self.KO_META[data_suffix][pid].insert(0, uid)
        # else:
        #     self.KO_META[data_suffix][pid] = [uid]

        self.store_meta(pid, uid, data_suffix)

        # Store Data to the commit cache
        self.KO_COMMIT_CACHE.insert(0, (uid, data_suffix, data))
        
        # Write Metadata and push the uncommited changes to the Meta file
        if not self.KO_NO_COMMIT:
            self.commit()

        # Initalizes table index if it's empty
        if data_suffix not in self.KO_INDEX:
            self.KO_INDEX[data_suffix] = {}

        # Stores index entry
        self.KO_INDEX[data_suffix][pid] = data
        return True

        # except Exception as e:
        #     print(e)
        #     print("Data save failed.({})".format(pid))
        #     return False


    def get(self, fid, data_suffix = None):
        """get(String:fid, String:data_suffix = None)

            Returns a database document.
            Options:
                fid = String:Required
                    > The id of the document to retrieve
                data_suffix = String:None
                    > The data suffix (table) on where to retrieve the document.
                        (Retrieves the default database if unspecified)
                        (***NOTE: This option is usually handled 
                                by the KO_Table Object.***)"""
        data_suffix = self.KO_DATA_SUFFIX if data_suffix is None else data_suffix
        if self.exists(fid, data_suffix):
            if data_suffix not in self.KO_INDEX:
                self.KO_INDEX[data_suffix] = {}

            if fid in self.KO_INDEX[data_suffix]:
                r = Map(self.KO_INDEX[data_suffix][fid])
                r["_id"] = fid
                return r

            # uid = self.KO_META[data_suffix][fid][0]
            uid = self.get_meta(fid, data_suffix)[0]
            # print("UID: {}".format(uid))

            data = getattr(yaml, self.KO_LOADTYPE)(self.KO_ZIP.read("{}.{}".format(uid, data_suffix)).decode("utf-8"))
            self.KO_INDEX[data_suffix][fid] = data # Add it to the memory
            data["_id"] = fid # Attach ID on the object
            return Map(data)
           
        return None


    def uid_generator(self):
        def generate():
            global KEYS
            return "".join(random.choice(KEYS) for x in range(0, 8))

    def commit(self):
        """commit()

            Commits the database to the file.
            Note: Doesn't need to be called if the no_commit option 
                    is set to True."""
        for item in self.KO_COMMIT_CACHE:
            self.KO_ZIP.writestr("{}.{}".format(item[0], item[1]), yaml.dump(dict(item[2])))

        # with open("{}.KO_META".format(self.KO_FILENAME), "w") as f:
        #     f.write(json.dumps(self.KO_META))

        for meta_chunk in range(0, len(self.KO_META)):
            if meta_chunk in self.KO_META_COMMIT_CACHE:
                chunk_filename = "{:08d}.meta".format(meta_chunk)
                with open(os.path.join(self.KO_FOLDER, "meta", chunk_filename), "w") as f:
                    f.write(json.dumps(self.KO_META[meta_chunk]))

        self.KO_COMMIT_CACHE.clear()
        self.KO_META_COMMIT_CACHE.clear()


    def init_meta(self):
        meta_filemask = os.path.join(self.KO_FOLDER, "meta", "*.meta")
        for file in glob.glob(meta_filemask):
            with open(file) as f:
                self.KO_META.append(json.loads(f.read()))

        chunk_size = 0
        for x in self.KO_META[-1]:
            for y in self.KO_META[-1][x]:
                try:
                    chunk_size += len(y)
                except:
                    pass

        self.KO_LAST_CHUNK_SIZE = chunk_size


    def store_meta(self, fid, data, data_suffix = None):
        """INTERNAL FUNCTION"""
        data_suffix = self.KO_DATA_SUFFIX if data_suffix is None else data_suffix

        for meta_chunk in range(0, len(self.KO_META)):
            if data_suffix in self.KO_META:
                if fid in self.KO_META[meta_chunk][data_suffix]:
                    self.KO_META[meta_chunk][data_suffix][fid].insert(0, data)
                    # Set this chunk to save on commit
                    self.KO_META_COMMIT_CACHE[meta_chunk] = True
                    return

        # Create a new chunk if the current chunk is full
        # print(self.KO_META)
        
        #print(chunk_size)
        #print(self.KO_META[-1].keys())
        if self.KO_LAST_CHUNK_SIZE >= self.KO_CONFIG["metasize"]:
            #print("NEW META ADDED")
            self.KO_META.append({})
            self.KO_LAST_CHUNK_SIZE = 0

        # IF the data is new, append to the last meta chunk
        if data_suffix not in self.KO_META[-1]:
            self.KO_META[-1][data_suffix] = {}

        if fid not in self.KO_META[-1][data_suffix]:
            self.KO_META[-1][data_suffix][fid] = []

        self.KO_META[-1][data_suffix][fid].insert(0, data)
        self.KO_LAST_CHUNK_SIZE += 1
        self.KO_META_COMMIT_CACHE[len(self.KO_META) - 1] = True
        return


    def get_meta(self, fid, data_suffix = None):
        """INTERNAL FUNCTION"""
        data_suffix = self.KO_DATA_SUFFIX if data_suffix is None else data_suffix
        for meta_chunk in range(len(self.KO_META) - 1, -1, -1):
            # print(meta_chunk)
            if data_suffix in self.KO_META[meta_chunk]:
                if fid in self.KO_META[meta_chunk][data_suffix]:
                    return self.KO_META[meta_chunk][data_suffix][fid]

        return None


    def items(self, data_suffix = None):
        """items(String:fid, String:data_suffix = None)

            Returns a list of the table's IDs
            Options:
                data_suffix = String:None
                    > The data suffix (table) on where to retrieve the document.
                        (Retrieves the default database if unspecified)
                        (***NOTE: This option is usually handled 
                                by the KO_Table Object.***)"""
        data_filter = data_suffix if data_suffix is not None else self.KO_DATA_SUFFIX
        meta = []
        for meta_chunk in self.KO_META:
            meta.extend(meta_chunk[data_filter].keys())
        return meta


    def tables(self):
        """tables()

            Returns a list of the database tables."""
        return self.KO_CONFIG["tables"]


class Ko_Table(object):
    """KoDB's Table Object"""
    def __init__(self, table_name, database):
        self.table_name = table_name
        self.database = database
        self.attr = None


    def KO_attr_processor(self, *args):
        args = list(args)
        args.append(self.table_name)
        args = tuple(args)

        return getattr(self.database, self.attr)(*args)


    def __getattr__(self, attr):
        self.attr = attr
        return self.KO_attr_processor



class Map(dict):
    def __init__(self, *args, **kwargs):
        super(Map, self).__init__(*args, **kwargs)
        for arg in args:
            if isinstance(arg, dict):
                for k, v in arg.items():
                    self[k] = v

        if kwargs:
            for k, v in kwargs.items():
                self[k] = v

    def __getattr__(self, attr):
        try:
            return self.get(attr)
        except:
            return None

    def __setattr__(self, key, value):
        self.__setitem__(key, value)

    def __setitem__(self, key, value):
        super(Map, self).__setitem__(key, value)
        self.__dict__.update({key: value})

    def __delattr__(self, item):
        self.__delitem__(item)

    def __delitem__(self, key):
        super(Map, self).__delitem__(key)
        del self.__dict__[key]
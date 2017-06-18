# Ko-DB a zip and yaml based database
# Author http://me.noku.space
import zipfile
import json
import yaml
import uuid


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
        self.KO_FILENAME = file
        try:
            self.KO_ZIP = zipfile.ZipFile(file, mode="a", allowZip64=True)
        except:
            self.KO_ZIP = zipfile.ZipFile(file, mode="x", allowZip64=True)

        self.KO_INDEX = {}
        if "load_to_memory" in OPTIONS:
            if OPTIONS["load_to_memory"]:
                self.load_all_to_memory()

        self.KO_NO_COMMIT = OPTIONS["no_commit"] if "no_commit" in OPTIONS else False


        self.KO_LOADTYPE = OPTIONS["load_type"] if "load_type" in OPTIONS else "safe_load"
        self.KO_DATA_SUFFIX = OPTIONS["data_suffix"] if "data_suffix" in OPTIONS else "default"
        # data_suffix = self.KO_DATA_SUFFIX if data_suffix is None else data_suffix
        # sets the function to the default table if none is specified
        
        try:
            with open("{}.KO_META".format(file)) as f:
                self.KO_META = json.loads(f.read())
        except:
            self.KO_META = {}

        try:
            with open("{}.KO_CONFIG".format(file)) as f:
                self.KO_CONFIG = yaml.load(f.read())
        except:
            self.KO_CONFIG = {}
            self.ko_set_config("tables", [self.KO_DATA_SUFFIX])


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
        with open("{}.KO_CONFIG".format(self.KO_FILENAME), "w") as f:
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
            if fid in self.KO_META[data_suffix]:
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
        try:
            data_suffix = self.KO_DATA_SUFFIX if data_suffix is None else data_suffix
            if data_suffix not in self.KO_META:
                self.KO_META[data_suffix] = {}

            uid = str(uuid.uuid4())
            if pid in self.KO_META[data_suffix]:
                self.KO_META[data_suffix][pid].insert(0, uid)
            else:
                self.KO_META[data_suffix][pid] = [uid]

            # Write Data to the database file
            self.KO_ZIP.writestr("{}.{}".format(uid, data_suffix), yaml.dump(dict(data)))

            # Write Metadata to the Meta file
            if not self.KO_NO_COMMIT:
                self.commit()

            # Initalizes table index if it's empty
            if data_suffix not in self.KO_INDEX:
                self.KO_INDEX[data_suffix] = {}

            # Stores index entry
            self.KO_INDEX[data_suffix][pid] = data
            return True

        except Exception as e:
            print(e)
            print("Data save failed.({})".format(pid))
            return False


    def commit(self):
        """commit()

            Commits the database to the file.
            Note: Doesn't need to be called if the no_commit option 
                    is set to True."""
        with open("{}.KO_META".format(self.KO_FILENAME), "w") as f:
            f.write(json.dumps(self.KO_META))


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
                return Map(self.KO_INDEX[data_suffix][fid])

            uid = self.KO_META[data_suffix][fid][0]

            data = getattr(yaml, self.KO_LOADTYPE)(self.KO_ZIP.read("{}.{}".format(uid, data_suffix)).decode("utf-8"))
            self.KO_INDEX[data_suffix][fid] = data # Add it to the memory
            data["_id"] = fid # Attach ID on the object
            return Map(data)
           
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
        return self.KO_META[data_filter].keys()


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
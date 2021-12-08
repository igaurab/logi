"""A log based database that indexes each key."""
import json
from loguru import logger

class Logi:
    """Log Based Database"""
    def __init__(self, db_uri: str = None, persist_index=True) -> None:
        self.DB_URI = db_uri or "/home/void/logi/db/database"
        self.INDEX_FILE = "/home/void/logi/db/index"
        self.persist_index = persist_index
        self.index = {}

    def set(self, key: str, value: str):
        """Append key,value in database file."""
        with open(self.DB_URI, "a+", encoding="utf-8") as db:
            current_pos = db.tell()
            value_start_pos = current_pos + len(key.encode('utf-8')) + 1
            value_offset = len(value.encode('utf-8'))
            self.index[key] = (value_start_pos, value_offset)
            log = f"{key},{value}\n"
            db.writelines(log)

        if self.persist_index:
            with open(self.INDEX_FILE, "w+", encoding="utf-8") as index_db:
                index_db.write(json.dumps(self.index))
    
    def _read_from_index(self, db, key):
        if not self.index:
            return None
        logger.info("Reading from Index.")
        value_start_pos, value_offset = self.index[key]
        db.seek(value_start_pos)
        value = db.read(value_offset)
        return value

    def _read_file(self, db, key):
        #TODO: Read file in reverse order. 
        # Since the log is append-only, have to read entire file to get latest value.
        logger.warning("Couldn't find index. Reading Entire File")
        for line in db.readlines():
            if line.startswith(key):
                value = line.split(",")[1]
        return value

    def get(self, key: str):
        """Get value associated with key."""
        read_strategy = [self._read_from_index, self._read_file]
        with open(self.DB_URI, "r", encoding="utf-8") as db:
            for read in read_strategy:
                value = read(db, key)
                if not isinstance(value, str): continue
                return value

if __name__ == "__main__":
    db = Logi(persist_index=False)
    # db.set("name","gaurab")
    # db.set("age","21")
    # db.set("name","saurav")
    value = db.get("name")
    logger.info(f"name: {value}")
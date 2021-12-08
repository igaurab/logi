"""A log based database that indexes each key."""

DATABASE_URI = "/home/void/logi/db/database"
INDEX_FILE = "/home/void/logi/db/index"

index = {}

def set(key: str, value: str):
    """Append key,value in database file"""
    with open(DATABASE_URI, "a+", encoding="utf-8") as db:
        current_pos = db.tell()
        value_start_pos = current_pos + len(key.encode('utf-8')) + 1
        value_offset = len(value.encode('utf-8'))
        index[key] = (value_start_pos, value_offset)
        db_write = f"{key},{value}\n"
        db.writelines(db_write)
        print("{} index: {}".format(key, index[key]))
#        print(db.tell())

def get(key: str):
    """Get value associated with key."""
    with open(DATABASE_URI, "r", encoding="utf-8") as db:
        value_start_pos, value_offset = index[key]
        db.seek(value_start_pos)
        value = db.read(value_offset)
        print(value)

set('name','gaurab')
set('age','21')
get('name')
get('age')

# Python imports:
from datetime import datetime
import time

time0 = time.time()

# Get the document properties:
table_name = Document.Properties["ModificationTable"]

Document.Data.Tables[table_name].ReloadAllData()
today = datetime.utcnow().strftime("%d/%m/%Y %H:%M:%S")
Document.Properties["Message"] = "{} UTC | Duration: {} seconds | Data reloaded.".format(today, round((time.time() - time0), 2))
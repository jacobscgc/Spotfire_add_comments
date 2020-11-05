from Spotfire.Dxp.Data.Export import DataWriterTypeIdentifiers 
from System.IO import File, Directory
from Spotfire.Dxp.Data import *
from Spotfire.Dxp.Application.Filters import *
# Python imports:
import time
from datetime import datetime

# Get the start time: (to measure how long it took)
time0 = time.time()


def save():
	# Reload the data to make sure that we are using the most recent data:
	Document.Data.Tables[table_name].ReloadAllData()

	stream = File.OpenWrite(fullPath)

	names = []
	for col in table.Columns:
	  names.append(col.Name)

	writer.Write(stream, table, filtering, names)
	today = datetime.utcnow().strftime("%d/%m/%Y %H:%M:%S")
	Document.Properties["Message"] = "{} UTC | Duration: {} seconds | Data was exported successfully.".format(today, round((time.time() - time0), 2))

	stream.Close()


def resetMarking():
        # Loop through each data table
        for dataTable in Document.Data.Tables:
            # Navigate through each marking in a given data table
            for marking in Document.Data.Markings:
                # Unmark the selection
                rows = RowSelection(IndexSet(dataTable.RowCount, False))
                marking.SetSelection(rows, dataTable)


# First, reset markings because if there are selected rows it will lead to an error when exporting:
resetMarking()

# Get the document properties:
fullPath = Document.Properties["SavePath"]
table_name = Document.Properties["ModificationTable"]

# Set the DataTable you want to use.
table = Document.Data.Tables[table_name]

# Create a excel exporter object:
writer = Document.Data.CreateDataWriter(DataWriterTypeIdentifiers.ExcelXlsDataWriter)

# Set a filtering or use a active one:
filtering = Document.ActiveFilteringSelectionReference.GetSelection(table).AsIndexSet()

# Try to save, if the file is busy, try 2 more times while waiting 10 seconds in between. If unable, message user that file is busy:
try:
	save()
except:
	time.sleep(10)  # sleep 10 seconds and try again
	try:
		save()
	except:
		time.sleep(10)
		try:
			save()
		except:
			today = datetime.utcnow().strftime("%d/%m/%Y %H:%M:%S")
			Document.Properties["Message"] = "{} UTC | Duration: {} seconds | Error, unable to save data after 3 attempts, file might be in use.".format(today, round((time.time() - time0), 2))
			print("File in use, could not save. Aborting after 3 attempts.")

# Spotfire specific imports:
from System.Collections.Generic import List
from Spotfire.Dxp.Data import *
from Spotfire.Dxp.Data.Transformations import *
# Python imports:
from datetime import datetime
import time
time0 = time.time()

# Create a cursor for the table column to get the values from.
# Add a reference to the data table in the script.
dataTable = Document.Data.Tables["FakeData"]
cursor = DataValueCursor.CreateFormatted(dataTable.Columns["UniqueID"])  # Use UniqueID because that is the identifier for a specific row

# Retrieve the marking selection
markings = Document.ActiveMarkingSelectionReference.GetSelection(dataTable)

# Create a List object to store the retrieved data marking selection
ids = List [str]()
row_indexes = List [int] ()

# Iterate through the data table rows to retrieve the UniqueID of the marked rows
for row in dataTable.GetRows(markings.AsIndexSet(),cursor):
	rowIndex = row.Index  # Get the index of the current row 
	value = cursor.CurrentValue  # Get the value of the current row
	# Only add if the id is not empty:
	if value <> str.Empty:
		ids.Add(value)  # Add the value of the UniqueID of the current row to the list
		row_indexes.Add(rowIndex)  # Add the index of the current row to the list

# Get only unique values (they should be unique anyway):
ids = list(set(ids))

# Check whether a row has been selected, if not, exit:
if len(ids) == 0:
	today = datetime.utcnow().strftime("%d/%m/%Y %H:%M:%S")
	Document.Properties["Message"] = "{} UTC | Duration: {} seconds | Error, no row has been selected.".format(today, round((time.time() - time0), 2))
else:  # NOTE: there is no friendly way to exit.. so everything has been placed inside else statement instead of placing an exit.
	# Specify in which column(s) replacements need to be made:
	column = DataColumnSignature("Comment", DataType.String)
	modify_column = DataColumnSignature("Comment_modification_date", DataType.String)
	# Set Comment to 'NA' because we want to delete it:
	Comment = "NA"
	# Set today:
	today = datetime.utcnow().strftime("%d/%m/%Y %H:%M:%S")

	dataOperation = dataTable.GenerateSourceView().OperationsSupportingTransformations[0];
	transformations = dataOperation.GetTransformations();

	# Retrieve the Column signature of the column identifying the row: 
	row_id_cols=[DataColumnSignature("UniqueID",DataType.String)]

	# Create a cursor for each column where values need to be replaced
	cursorValue = DataValueCursor.CreateFormatted(dataTable.Columns["Comment"])
	cursorValue2 = DataValueCursor.CreateFormatted(dataTable.Columns["Comment_modification_date"])

	update_list_of_lists = []
	# Loop through the selected rows and replace the values with the current comment:
	for i in range(len(ids)):
		row_key = ids[i]
		print row_key
		
		# Get existing value based on this key:
		rowSelection = dataTable.Select('UniqueID = ' + '"' + row_key + '"')  # extract the row with this UniqueID
		# From this row, get the Comment:
		for  row in  dataTable.GetRows(rowSelection.AsIndexSet(),cursorValue):
			currentOverrideValue = cursorValue.CurrentValue
			break
		# Next, get the Comment_modification_date:
		for  row in  dataTable.GetRows(rowSelection.AsIndexSet(),cursorValue2):
			today_override_value = cursorValue2.CurrentValue
			break
		row_id_col_values = [row_key]  # row which needs to be replaced
		# Add the planned changes to the update list (only those values that might change between different lines):
		update_list_of_lists.append([currentOverrideValue, row_id_col_values, today_override_value])

	# Next, update the data table and loop once more to check whether the values have been changed by someone else. If not, apply changes.
	Document.Data.Tables["FakeData"].ReloadAllData()
	for currentOverrideValue, row_id_col_values, today_override_value in update_list_of_lists:
		# Check the modification date after the reload, Get existing value based on this key:
		row_key = row_id_col_values[0]
		rowSelection = dataTable.Select('UniqueID = ' + '"' + row_key + '"')  # extract the row with this UniqueID
		for  row in  dataTable.GetRows(rowSelection.AsIndexSet(),cursorValue2):
			today_override_value2 = cursorValue2.CurrentValue
			break
		if today_override_value != today_override_value2:
			Document.Properties["Message"] = "{} UTC | Duration: {} seconds |Comment was updated by someone else, skipping..".format(today, time.time() - time0)
			print "Comment was updated by someone else"
		else:
			transformations.Add(ReplaceSpecificValueTransformation(column, currentOverrideValue, Comment, row_id_cols, row_id_col_values, False))  # Remove comment
			transformations.Add(ReplaceSpecificValueTransformation(modify_column, today_override_value, "NA", row_id_cols, row_id_col_values, False))  # Remove modification date/time

	# Apply all transformations created in the for loop:
	dataOperation.ReplaceTransformations(transformations)
	Document.Properties["Message"] = "{} UTC | Duration: {} seconds | Comment was deleted.".format(today, round((time.time() - time0), 2), Comment)

# Spotfire specific imports:
from System.Collections.Generic import List
from Spotfire.Dxp.Data import *
from Spotfire.Dxp.Data.Transformations import *
from System.Threading import Thread  # To identify user
# Python imports:
from datetime import datetime
import time

# Get the start time: (to measure how long it took)
time0 = time.time()

# Get the document properties:
Comment = Document.Properties["Comment"]
table_name = Document.Properties["ModificationTable"]
id_column = Document.Properties["IDColumn"]
comment_column = Document.Properties["CommentColumn"]
modification_date_column = Document.Properties["ModificationDateColumn"]

# Add a reference to the data table in the script, it uses the ModificationTable property.
dataTable = Document.Data.Tables[table_name]
# Create a cursor for the table column to get the values from, it uses the IDColumn property to identify in which line it should add the comment.
cursor = DataValueCursor.CreateFormatted(dataTable.Columns[id_column])


# Identify the User:
User = Thread.CurrentPrincipal.Identity.Name

# Retrieve the marking selection (in other words, which lines/rows were selected?)
markings = Document.ActiveMarkingSelectionReference.GetSelection(dataTable)

# Create a List object to store the retrieved data marking selection (which lines/rows were selected)
ids = List [str]()

# Iterate through the data table rows to retrieve the unique id of the marked/selected rows
for row in dataTable.GetRows(markings.AsIndexSet(),cursor):
	value = cursor.CurrentValue  # Get the value of the current row
	# Only add if the id is not empty:
	if value <> str.Empty:
		ids.Add(value)  # Add the value of the UniqueID of the current row to the list

# Get only unique values (they should be unique anyway):
ids = list(set(ids))

# Check whether a row has been selected, if not, skip the rest of the script:
if len(ids) == 0:
	today = datetime.utcnow().strftime("%d/%m/%Y %H:%M:%S")
	Document.Properties["Message"] = "{} UTC | Duration: {} seconds | Error, no row has been selected.".format(today, round((time.time() - time0), 2))  # NOTE: 'time.time() - time0' gives the time the script took from the time time0 was initiated until now in seconds.
else:  # NOTE: there is no friendly way to exit.. so everything has been placed inside else statement instead of placing an exit.
	# Specify in which column(s) replacements need to be made:
	column = DataColumnSignature(comment_column, DataType.String)
	modify_column = DataColumnSignature(modification_date_column, DataType.String)
	# Get the current date/time:
	today = datetime.utcnow().strftime("%d/%m/%Y %H:%M:%S")

	dataOperation = dataTable.GenerateSourceView().OperationsSupportingTransformations[0];
	transformations = dataOperation.GetTransformations();

	# Retrieve the Column signature of the column identifying the row: 
	row_id_cols=[DataColumnSignature(id_column,DataType.String)]

	# Create a cursor for each column where values need to be replaced
	cursorValue = DataValueCursor.CreateFormatted(dataTable.Columns[comment_column])
	cursorValue2 = DataValueCursor.CreateFormatted(dataTable.Columns[modification_date_column])

	update_list_of_lists = []
	# Loop through the selected rows and replace the values with the current comment:
	for i in range(len(ids)):
		row_key = ids[i]
		# Get existing value based on this key which extracts the row based on the unique id:
		rowSelection = dataTable.Select(id_column + ' = ' + '"' + row_key + '"')
		# From this row, get the Comment, so 'currentOverrideValue' contains the current value of the comment column for this row.
		for row in dataTable.GetRows(rowSelection.AsIndexSet(),cursorValue):
			currentOverrideValue = cursorValue.CurrentValue
			break
		# Next, get the Comment_modification_date (same as above but for a different column) so 'today_override_value' contains the current value of the modification date:
		for row in dataTable.GetRows(rowSelection.AsIndexSet(),cursorValue2):
			today_override_value = cursorValue2.CurrentValue
			break
		row_id_col_values = [row_key]  # identifier for which row needs to be replaced, has to be a list
		# Add the planned changes to the update list (only those values that might change between different lines):
		update_list_of_lists.append([currentOverrideValue, row_id_col_values, today_override_value])

	# Next, update the data table (refresh data) and loop once more to check whether the values have been changed by someone else. If not, apply changes.
	Document.Data.Tables[table_name].ReloadAllData()  # data refresh
	for currentOverrideValue, row_id_col_values, today_override_value in update_list_of_lists:
		# The next code block extracts the modification date again as 'today_override_value2'. It then checks whether this is different from 'today_override_value'.
		# If it is different, it means that someone else modified this comment after the last data refresh of this user and the comment will not be added.
		row_key = row_id_col_values[0]
		rowSelection = dataTable.Select(id_column + ' = ' + '"' + row_key + '"')  # extract the row with this unique id
		for row in dataTable.GetRows(rowSelection.AsIndexSet(),cursorValue2):
			today_override_value2 = cursorValue2.CurrentValue
			break
		if today_override_value != today_override_value2:
			Document.Properties["Message"] = "{} UTC | Duration: {} seconds | Comment was updated by someone else, skipping..".format(today, time.time() - time0)
			print "Comment was updated by someone else"
		# If the comment did not change since the last refresh of the User, add the comment and comment modification date:
		else:
			Comment = "{} | {} ({})".format(currentOverrideValue, Comment, User)
			transformations.Add(ReplaceSpecificValueTransformation(column, currentOverrideValue, Comment, row_id_cols, row_id_col_values, False))  # Extend comment
			transformations.Add(ReplaceSpecificValueTransformation(modify_column, today_override_value, today, row_id_cols, row_id_col_values, False))  # Update modification date/time

	# Apply all transformations created in the for loop:
	dataOperation.ReplaceTransformations(transformations)
	Document.Properties["Message"] = "{} UTC | Duration: {} seconds | Comment  was extended to: '{}'.".format(today, round((time.time() - time0), 2), Comment)

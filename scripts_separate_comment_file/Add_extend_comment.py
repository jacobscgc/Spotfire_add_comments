# Spotfire imports:
from Spotfire.Dxp.Data import *
from Spotfire.Dxp.Data.Import import * 
from System.Threading import Thread  # to identify the User
# Python imports:
from datetime import datetime
import time

# ### FUNCTIONS ###

def read_csv(file_loc):
	"""
	This function reads the comment file and returns all comments as a dictionary with batch number as key.
	"""
	comments = {}
	with open(file_loc, 'r') as f:
		next(f)  # skip header
		# Loop through entries in the comment file:
		for line in f:
			# Retrieve the comments:
			batch, comment, user, mod_date, match_date = line.rstrip().split(',')
			# Add comment to library:
			comments[batch] = [comment, user, mod_date, match_date]
	return comments


def write_csv(file_loc, comment_dictionary):
	"""
	This function writes the comment dictionary to csv.
	"""
	# Open the csv as write (will overwrite existing):
	write_object = open(file_loc, 'w')
	# Write the header:
	write_object.write('Batch,Comment,User,Comment_modification_date,Most_recent_match_date\n')
	# Write the dictionary to file:
	for key in comment_dictionary:  # Key = batch number
		comment, user, mod_date, match_date = comment_dictionary[key]
		write_object.write('{0},{1},{2},{3},{4}\n'.format(key, comment, user, mod_date, match_date))
	write_object.close()


def retrieve_selected_rows(input_table_name, input_id_column):
	"""
	This function checks which rows in the input table have been selected and returns the values of the IDColumn in the form of a list.
	"""
	# Add a reference to the source data table in the script.
	sourceTable = Document.Data.Tables[input_table_name]

	# Create a cursor for the table column to get the values from, it uses the IDColumn property to identify in which line it should add the comment.
	source_cursor = DataValueCursor.CreateFormatted(sourceTable.Columns[input_id_column])

	# Retrieve the marking selection (in other words, which lines/rows were selected?)
	markings = Document.ActiveMarkingSelectionReference.GetSelection(sourceTable)

	# Create a List object to store the retrieved data marking selection (which lines/rows were selected)
	ids = []

	# Iterate through the source data table rows to retrieve the unique id of the marked/selected rows
	for row in sourceTable.GetRows(markings.AsIndexSet(),source_cursor):
		value = source_cursor.CurrentValue  # Get the value of the current row
		if value not in ids:
			ids.append(value)

	return ids


def extract_current_values(input_table_name, input_id_column, input_comment_column, input_comment_change_column, input_user_column, input_match_column, input_id_list):
	"""
	This function checks whether the values in the id_list are already present in the comment table or not. 
	If present, it returns the current values of the entry. It returns a dictionary with the results, where batch is key.
	"""
	# Create an instance of the table where we need to check whether the comment exists.
	checkTable = Document.Data.Tables[input_table_name]
	# Create a cursor for the id_column, comment_column and comment_change_column.
	id_cursor = DataValueCursor.CreateFormatted(checkTable.Columns[input_id_column])
	comment_cursor = DataValueCursor.CreateFormatted(checkTable.Columns[input_comment_column])
	comment_change_cursor = DataValueCursor.CreateFormatted(checkTable.Columns[input_comment_change_column])
	user_change_cursor = DataValueCursor.CreateFormatted(checkTable.Columns[input_user_column])
	match_date_cursor = DataValueCursor.CreateFormatted(checkTable.Columns[input_match_column])
	# Create a list of all id's in the comment file:
	ids = [id_cursor.CurrentValue for i in checkTable.GetRows(id_cursor)]
	# Create an output dictionary:
	output = {}
	# Check for each id in the id_list whether an entry for this id exists:
	for id_it in input_id_list:
		if id_it in ids:  # the id already exists, extract the current values
			# Get existing value based on this key which extracts the row based on the unique id:
			rowSelection = checkTable.Select(input_id_column + ' = ' + '"' + id_it + '"')
			# From this row, get the Comment, so 'int_comment' contains the current value of the comment column for this row.
			for row in checkTable.GetRows(rowSelection.AsIndexSet(),comment_cursor):
				int_comment = comment_cursor.CurrentValue
				break
			# Next, get the Comment_modification_date (same as above but for a different column) so 'comment_mod' contains the current value of the modification date:
			for row in checkTable.GetRows(rowSelection.AsIndexSet(),comment_change_cursor):
				comment_mod = comment_change_cursor.CurrentValue
				break
			# Get the current value for the User:
			for row in checkTable.GetRows(rowSelection.AsIndexSet(),user_change_cursor):
				prev_user = user_change_cursor.CurrentValue
				break
			# Get the current value for the modification date:
			for row in checkTable.GetRows(rowSelection.AsIndexSet(),match_date_cursor):
				prev_match = match_date_cursor.CurrentValue
				break
			# Add the values to the dict:
			output[id_it] = [int_comment, prev_user, comment_mod, prev_match]
	# Finally, return the output list:
	return output


def add_extend_comments():
	"""
	This function loops through the ids where the Comment should be added or extended. For each id, check whether there was already a comment. If so, check whether the comment
	in the comment table is the same as in the file or whether it was changed by a different user in the meanwhile. If it didn't change, update it. Otherwise ignore it.
	Finally, it tries to write the changes to file. If it cannot write the changes to file it means the file is busy because someone else was adding comments.
	"""
	# Read the comment file to a dictionary:
	comment_dict = read_csv(file_location)

	# Loop through the ids where the Comment should be added or replaced. For each id, check whether there was already a comment. If so, check whether the comment
	# in the comment table is the same as in the file or whether it was changed by a different user in the meanwhile. If it didn't change, update it. Otherwise ignore it.
	batch_number = len(ids)
	failed = 0
	for id_entry in ids:
		if id_entry in current_val_dict:  # there was already a comment for this id
			# Check whether the data file and the spotfire table contain the same data, don't check the match date because it isn't used yet.
			if current_val_dict[id_entry][0:2] == comment_dict[id_entry][0:2]:  # spotfire table and file have same entry, extend:
				old_comment = current_val_dict[id_entry][0]
				mod_date_new = datetime.utcnow().strftime("%d/%m/%Y %H:%M:%S")  # update timestamp for modification date
				updated_comment = '{} | {}'.format(old_comment, Comment)  # add the new comment
				match_date = datetime.utcnow().strftime("%d/%m/%Y")  # update match date, as we know it was matched at this moment anyway
				comment_dict[id_entry] = [updated_comment, User, mod_date_new, match_date]  # update entry in dict, match date not used for now
			else:
				failed += 1
				print "Value on disk and sportfire table differ"
		else:  # Comment didn't exist yet, add it:
			mod_date_new = datetime.utcnow().strftime("%d/%m/%Y %H:%M:%S")  # update timestamp for modification date
			match_date = datetime.utcnow().strftime("%d/%m/%Y")
			comment_dict[id_entry] = [Comment, User, mod_date_new, match_date]  # add entry to dict. 

	# Save changes:
	write_csv(file_location, comment_dict)

	# Write message:
	today = datetime.utcnow().strftime("%d/%m/%Y %H:%M:%S")
	if failed == 0:
		Document.Properties["Message"] = "{} UTC | Duration: {} seconds | Comment '{}' was added/extended to {} selected rows.".format(today, round((time.time() - time0), 2), Comment, batch_number)
	else:
		Document.Properties["Message"] = "{} UTC | Duration: {} seconds | Unable to add/extend comment '{}' for {} out of {} selected rows due to updated source file.".format(today, round((time.time() - time0), 2), Comment, failed, batch_number)

	# Reload the comment table:
	Document.Data.Tables[table_name].ReloadAllData()


# ### END FUNCTIONS ###

# Start time:
time0 = time.time()

# Get the document properties:
Comment = Document.Properties["Comment"]
table_name = Document.Properties["ModificationTable"]
source_table = Document.Properties["SourceTable"]
id_column = Document.Properties["IDColumn"]  # this should be the same between the source and modification table
comment_column = Document.Properties["CommentColumn"]
modification_date_column = Document.Properties["ModificationDateColumn"]
file_location = Document.Properties["SavePath"]
User = Thread.CurrentPrincipal.Identity.Name

# Extract which lines were selected in the source table:
ids = retrieve_selected_rows(source_table, id_column)

# Obtain the current values of the comment table for the selected ids (if they exist, this is to compare them to what was found in the file)
current_val_dict = extract_current_values(table_name, id_column, comment_column, modification_date_column, "User", "Most_recent_match_date", ids)

# Try to update the comments, if the output file is busy, wait 10 seconds and try again. If it cannot process it within 3 attempts, abort and write a message to the User that the file is busy.
try:
	add_extend_comments()
except:
	time.sleep(10)
	try:
		add_extend_comments()
	except:
		time.sleep(10)
		try:
			add_extend_comments()
		except:
			today = datetime.utcnow().strftime("%d/%m/%Y %H:%M:%S")
			Document.Properties["Message"] = "{} UTC | Duration: {} seconds | Unable to add/replace comments, output file in use.".format(today, round((time.time() - time0), 2))
print 'Script finished in {} seconds.'.format(time.time() - time0)

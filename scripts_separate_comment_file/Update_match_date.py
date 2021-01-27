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


def match_batches():
	"""
	This function checks whether the batch numbers in the comment table still exist in the source table. If so, the match date is updated. Otherwise, it is kept the same.
	"""
	# Read the comment file to a dictionary:
	comment_dict = read_csv(file_location)

	# Create an instance of the table where we need to check whether the batch exists.
	matchTable = Document.Data.Tables[source_table]
	commentTable = Document.Data.Tables[table_name]

	# Create a cursor for the id_column for both tables.
	id_cursor_match = DataValueCursor.CreateFormatted(matchTable.Columns[id_column])
	id_cursor_comment = DataValueCursor.CreateFormatted(commentTable.Columns[id_column])

	# Create a list of all id's in the comment table:
	ids_match = [id_cursor_match.CurrentValue for i in matchTable.GetRows(id_cursor_match)]
	ids_comment = [id_cursor_comment.CurrentValue for i in commentTable.GetRows(id_cursor_comment)]

	# Obtain the current values of the comment table for all ids (this is to compare them to what was found in the file)
	current_val_dict = extract_current_values(table_name, id_column, comment_column, modification_date_column, "User", "Most_recent_match_date", ids_comment)

	# Loop through the comment ids and check whether this id can be found in the source table too (ids_match). If so, update the match date. Otherwise leave it as is.
	batch_number = len(ids_comment)
	failed = 0
	unmatched = 0
	for id_entry in ids_comment:
		if id_entry in ids_match:  # this batch still exists in  the source table
			match_date = datetime.utcnow().strftime("%d/%m/%Y")
			# Make sure that it didn't change in the source:
			if current_val_dict[id_entry][0:2] == comment_dict[id_entry][0:2]:  # spotfire table and file have same entry, update match date:
				mod_date = current_val_dict[id_entry][2]
				comment = current_val_dict[id_entry][0]
				user = current_val_dict[id_entry][1]
				comment_dict[id_entry] = [comment, user, mod_date, match_date]  # update entry in dict
			else:
				failed += 1
				print "Entry {} changed while matching, not updated".format(id_entry)
		else:
			unmatched += 1  # id could not be matched

	# Write the comment dictionary to file again:
	write_csv(file_location, comment_dict)

	# Write message:
	today = datetime.utcnow().strftime("%d/%m/%Y %H:%M:%S")
	if failed == 0:
		Document.Properties["Message"] = "{} UTC | Duration: {} seconds | Matching completed, {} batches were not matched.".format(today, round((time.time() - time0), 2), unmatched)
	else:
		Document.Properties["Message"] = "{} UTC | Duration: {} seconds | Unable to perform matching for {} out of {} batch numbers due to updated source file. {} batches were not matched.".format(today, round((time.time() - time0), 2), failed, batch_number, unmatched)

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

# Try to match, if the output file is busy, wait 10 seconds and try again. If it cannot process it within 3 attempts, abort and write a message to the User that the file is busy.
try:
	match_batches()
except:
	time.sleep(10)
	try:
		match_batches()
	except:
		time.sleep(10)
		try:
			match_batches()
		except:
			today = datetime.utcnow().strftime("%d/%m/%Y %H:%M:%S")
			Document.Properties["Message"] = "{} UTC | Duration: {} seconds | Unable to complete matching batch numbers, output file in use.".format(today, round((time.time() - time0), 2))
print 'Script finished in {} seconds.'.format(time.time() - time0)

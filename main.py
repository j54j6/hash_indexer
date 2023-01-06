import os
import sys
import json
import sqlite3
from sqlite3 import Error
import hashlib
import re
import multiprocessing as mp

#Directory to work in - all subfolders will be included
root_dir = "//nas.jr.local/data"

#DB File - all data are saved in here
db_file ="./test/data.db"
#Gloabal DB Connection Object to use in all created threads
global conn
#Set conn to None
conn = None
#Buffer per Thread in Bytes - 2GB per Thread
BUF_SIZE = 2147483648 

#Open SQLite File and init. conn Object for further use. !Disabled smae check funcionality!
def create_connection(db_file):
    """ create a database connection to a SQLite database """
    conn = None
    try:
        conn = sqlite3.connect(db_file, check_same_thread=False)
        return conn
    except Error as e:
        print(e)
        return False

#create a new SQLite DB with important tables 
def create_new_db(conn):
    #check if there is a variable set as parameter if not return false - There is a conn var needed to pass (besided there is a gloabal object)
    if conn == None:
        print("No active connection!")
        return False

    #Create a cursor to execute queries
    c = conn.cursor()
    #File table used for all files that are saved and the hash files
    c.execute('''
          CREATE TABLE IF NOT EXISTS files
          ([ID] INTEGER PRIMARY KEY, [file_name] TEXT, [hash] TEXT, UNIQUE(file_name))
          ''')      
    #For multithreading there is a lock needed to create multiple threads.               
    c.execute('''
          CREATE TABLE IF NOT EXISTS indexer_cache
          ([ID] INTEGER PRIMARY KEY, [file_name] TEXT, [locked] BOOL, UNIQUE(file_name))
          ''')   
    conn.commit()

#check if the given filenam e(mainlöy a complete absolute path) already saved in the SQLite database
def check_for_file_in_db(filename):
    #check if there is a given filename and if the global conn object is initialized
    if filename == None or conn == None:
        print("NO DB or No Filename!")
        return False
    #Query
    command = "SELECT 1 FROM files WHERE file_name= ?"
    result = conn.execute(command, [filename]).fetchone()
    if result:
        return True
    else:
        return False

#Add a new file to the DB. Save the filename and the hash
def add_file_to_db(file, hash):
    cursor = conn.cursor()
    cursor.execute("INSERT INTO files (file_name, hash) VALUES( ? , ?)", [file, hash])
    conn.commit()
    cursor.close()
    if not cursor.rowcount:
        print("Error while adding File " + file + " to database!")

#If a file is beeing computed by a thread this file needs to be locked (no need to compute the same file in two or more processes) - Lock the file with adding to indexer_cache table
def indexer_lock_file(file):
    cursor = conn.cursor()
    cursor.execute("INSERT INTO indexer_cache (file_name, locked) VALUES( ? , ?)", [file, True])
    conn.commit()
    cursor.close()
    if not cursor.rowcount:
        print("Error while locking File " + file + "!")

#If the file is computed unlock the file - remove it from indexer_cache tabvle
def indexer_unlock_file(file):
    cursor = conn.cursor()
    cursor.execute("DELETE FROM indexer_cache WHERE file_name = ?", [file])
    conn.commit()
    cursor.close()
    if not cursor.rowcount:
        print("Error while unlock File " + file + "!")

#check if a file is already locked - check for entry in indexer_cache table
def indexer_check_if_file_is_locked(file):
    if file == None or conn == None:
        print("NO DB or No Filename!")
        return False
    command = "SELECT 1 FROM indexer_cache WHERE file_name= ?"
    result = conn.execute(command, [file]).fetchone()
    if result:
        return True
    else:
        return False

#if there are any errors in a previous run it is possible that there are locks remaining - remove them at the start of the script - these files can be recomputed        
def indexer_clear_cache():
    cursor = conn.cursor()
    cursor.execute("DELETE FROM indexer_cache WHERE file_name != ''")
    conn.commit()
    cursor.close()
    if not cursor.rowcount:
        print("Error while clearing cache!")

#Create a hash from a given file
def create_hash_from_file(file):
    #create hash and return the hex value
    hash_obj = hashlib.sha256()
    with open(file, 'rb') as f: # Open the file to read it's bytes
        fb = f.read(BUF_SIZE) # Read from the file. Take in the amount declared above
        while len(fb) > 0: # While there is still data being read from the file
            hash_obj.update(fb) # Update the hash
            fb = f.read(BUF_SIZE) # Read the next block from the file
    return [file, hash_obj.hexdigest()]

#This function handles the async worker results. It saves the returned hash value with the filename to the SQLite3 Database
def indexer_save_generated_value(result):
    add_file_to_db(result[0], result[1])
    indexer_unlock_file(result[0])

#If there is an error during execution in a worker this function is called. -NOT Finished yet -> Ideas: Auto unlock computed file (how to get var?) - try error handling and fixing?
def handle_error(*data):
    print("Error in workerprocess!")
    print("Sended Error: ", data)

#check if there are any new files in the given directory and compute them (Indexer)
def check_for_new_files(work_dir):
    #check if the given path exists
    if not os.path.exists(work_dir):
        return False
    else:
        #create an array to save all files and sub folders
        all_files = []
        #add all files to an array
        for folder, subs, files in os.walk(work_dir):
            all_files.append({"dir_name": folder, "files": files})

        #iterate over the array and test if the files are existing in DB
        for folder in all_files:
            #get the avsolute path to save this into the database (much easier if moved to another machine, the values can be recomputed e.G. for a NAS Storage, only the entrypoint need to be changed e.g Windows: \\nas.local to Linux /mnt/nas or /nas)
            folder_path = os.path.abspath(folder["dir_name"])
            #create a new pool for async worker handling
            pool = mp.Pool()
            #iterate over all folders and compute them
            for file_name in folder["files"]:
                #get the full absolute path and filename to save them correctly to db
                full_file_name = os.path.join(folder_path, file_name)
                #check if the file already exist in the db
                file_exist = check_for_file_in_db(full_file_name)
                if not file_exist:
                    #if the file does not exist check iff the file is already computed
                    file_locked = indexer_check_if_file_is_locked(full_file_name)
                    if not file_locked:
                        #if not cokmputed add it to the current pool
                        pool.apply_async(create_hash_from_file, args=([full_file_name]), callback=indexer_save_generated_value, error_callback=handle_error)
                else:
                    continue
            #close the current pool - all files of the folder are in the pool, no tasks need to be added anymore
            pool.close()
            #all files in the current folder are added to the pool wait until all files are computed then go to the next folder 
            pool.join()

#Start
if __name__ == '__main__':
    #check if the given path and db file exists
    if os.path.exists(os.path.abspath(db_file)):
        #if the path exist create a connection object for the SQLite3 DB
        conn = create_connection(db_file)
        #clear cache to supress earlier errors and compute all files trhat are not in the DB
        indexer_clear_cache()
        #checck for new files and do all the needed stuff
        check_for_new_files(root_dir)
    else:
        #if the db file doesn't exist - create it and exit
        f = open(db_file, "x")
        conn = create_connection(db_file)
        create_new_db(conn)

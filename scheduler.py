#! python

##
## Schedule processes, with both timestamps and dependencies: 
## a mixture of make and crontab.
##
## (c) 2013 Vincent Zoonekynd <zoonek@gmail.com>
## Licence: GPL3
## 
## Each job is described in a file, as follows: 
##
##   $ cat update1
##   command:     apt-get update
##   start_after: 05:00
##   days:        0
##
##   $ cat update2
##   command:     apt-get upgrade
##   start_after: 05:00
##   days:        0
##   depends_on:  /tmp/2013-03-conf/update1
##
## Terminology used:
## - job:     description of a job
## - process: what is actually run, anew, every day -- there is one for every day, and it has a state.
##
## The data is read, and stored in a database (currently SQLite -- 
## since I store arrays as strings, this is very suboptimal -- a key-value store,
## such as MongoDB would have been a better choice).
## There is one table for jobs, and one for statuses.
## The status of each job (for each date) is also stored in the same database.
## The possible statuses are: 
##   waiting    (the process is waiting for some condition, either the time or some deoendency; 
##   invalid    (the process has an invalid dependency: it will never be run)
##   ready      (processes will not stay in this state for very long, unless I add some ressource management)
##   running    (the process was launched and is probably still running)
##   done       (the process finished, its return code was 0)
##   failed     (the process finished, its return code was not 0)
##   unknown    (was apparently running when the server died)
##   stopped    (stopped by the user -- not implemented)
##   deleted    (the job no longer exists)
##   missing    (unknown process)
##
## A process will only run if its dependencies are in the "done" state, 
## and of its start time is in the past.
##
## Missing features (they will probably never be implemented):
## - Check that the dependencies present no cycle.
## - Ability to check (e.g., on a web page): the status of a process, 
##   how long it has been running, what it is waiting for, 
##   which processes are waiting for it.
## - Ability to plot the dependencies of a process.
## - Ability to stop/park/unpark/force a process (e.g., from the same GUI, or from the command-line)
## - Display historical data 
##
##########################################################################################

import os
import sys
import sqlite3
import subprocess
from time            import gmtime, strftime, sleep
from multiprocessing import Process, Queue

##
## Parameters and global variables
##

# Directories and files
configuration = "/tmp/2013-03-conf"
logs          = "/tmp/2013-03-log"
database_dir  = "/tmp/2013-03-db"
database      = database_dir + "/" + "a.db"

if not os.path.isdir(configuration):
  print "Missing configuration directory:", configuration
  sys.exit()
if not os.path.isdir(logs):
  print "Missing log directory:", logs
  sys.exit()
if not os.path.isdir(database_dir):
  print "Missing database directory:", database_dir
  sys.exit()

# Date
if len(sys.argv) != 2:
    print "Usage: ", sys.argv[0], "date"
    sys.exit()

rundate = sys.argv[1] # The date, e.g., "2013-03-06", is passed as argument

connection = sqlite3.connect( database )
cursor     = connection.cursor()

##########################################################################################

##
## Constants
## 

WAITING = "waiting"
READY   = "ready"
RUNNING = "running"
DONE    = "done"
FAILED  = "failed"
UNKNOWN = "unknown"
STOPPED = "stopped"
DELETED = "deleted"
MISSING = "missing"  
INVALID = "invalid"

##
## Spawning a new process
##

def run_command( command, log_file, queue = Queue() ): 
  actual_command = "( " + command + " ) 2>&1 >" + log_file
  print "LOG run_command actual_command:", actual_command
  result = subprocess.call( actual_command, shell = True)
  queue.put( result )
  # TODO: Find a way of getting the return code of the command...

##
## DDL
##

def create_status_table():
  cursor.execute( '''
    CREATE TABLE IF NOT EXISTS status (pk INTEGER PRIMARY KEY AUTOINCREMENT, id, rundate, time, status, comment)
  ''' )
  cursor.execute( "DROP VIEW IF EXISTS current_status" )
  cursor.execute( '''
    CREATE VIEW current_status AS
    SELECT A.id AS id, A.rundate AS rundate, time, status, comment
    FROM ( SELECT id, rundate, MAX(pk) AS pk             FROM status GROUP BY id, rundate ) A,
         ( SELECT id, rundate, pk, time, status, comment FROM status ) B         
    WHERE A.id      = B.id
    AND   A.pk      = B.pk
    AND   A.rundate = B.rundate
  ''' )
  connection.commit()

def create_job_table(): 
  cursor.execute( '''
    CREATE TABLE IF NOT EXISTS job (id PRIMARY KEY, description, command, start_after, days, depends_on)    
  ''' )
  connection.commit()

##
## Getters
##

def get_job_description(id):
  cursor.execute( "SELECT id, description, command, start_after, days, depends_on FROM job WHERE id=?", (id,) )
  job = cursor.fetchone()
  return { 
    "id":          job[0],
    "description": job[1], 
    "command":     job[2], 
    "start_after": job[3], 
    "days":        job[4], 
    "depends_on":  [ u.strip() for u in job[5].split(",") if u.strip() != "" ]
  }

def get_process(id, rundate):
  cursor.execute( 
    "SELECT id, rundate, time, status, comment FROM current_status WHERE id=? AND rundate=?", 
    (id,rundate,) 
  )
  row = cursor.fetchone()
  #print "LOG get_process", row
  if row == None:
    return row
  process = {
    "id":      row[0],
    "rundate": row[1],
    "time":    row[2],
    "status":  row[3],
    "comment": row[4]
  }
  #print "LOG get_process", process
  return process

def get_process_status(id, rundate):
  process = get_process(id, rundate)
  #print "LOG process_status  id =", id, " rundate =", rundate, " process = ", process
  if process == None:
    return MISSING
  return process[ "status" ]

def get_job_count(): 
  cursor.execute( "SELECT COUNT(*) AS n FROM job" )
  return cursor.fetchone()[0]

def get_jobs_from_status(status, rundate):
  cursor.execute( 
    "SELECT id FROM current_status WHERE status = ? AND rundate = ?",
    (status,rundate,)
  )
  return set([ u[0] for u in cursor.fetchall() ])

def get_running_jobs(rundate): 
  return get_jobs_from_status( RUNNING, rundate )

def get_all_jobs():
  cursor.execute( "SELECT id FROM job" )
  return set([ u[0] for u in cursor.fetchall() ])

def get_current_processes(rundate):
  cursor.execute( "SELECT id FROM current_status WHERE rundate = ? AND status != ?", (rundate,DELETED) )
  return set([ u[0] for u in cursor.fetchall() ])

def get_timestamp(date = None, time = None): 
  if date == None:
    return strftime("%Y-%m-%d %H:%M:%S +0000", gmtime())
  # TODO: need for less trivial date arithmetic, e.g., "06:00+1", "06:00+1b", "30:00", "06:00 London"
  #print "LOG get_timestamp:", date + " " + time + " +0000"
  return date + " " + time + " +0000"

def get_dependency_graph():
  adjacency_list = {}
  for job_id in get_all_jobs():
    adjacency_list[ job_id ] = get_job_description( job_id )[ "depends_on" ]
  return adjacency_list;

##
## DML
##

def set_job_status(id, rundate, new_status, comment=""):
  timestamp = get_timestamp()
  cursor.execute(
    "INSERT INTO status (id, rundate, time, status, comment) VALUES (?,?,?,?,?)",
    (id,rundate,timestamp,new_status,comment,)
  )

def add_job( job ):
  cursor.execute(
    "INSERT INTO job (id, description, command, start_after, days, depends_on) VALUES (?,?,?,?,?,?)",
    (job["id"], job["description"], job["command"], job["start_after"], job["days"], job["depends_on"],)
  )

##
## Inputs
## 

def read_file( filename ):
  print "Processing file ", filename
  f = open( filename, 'r' )
  result = {}
  for line in f: 
    if line.startswith( "#" ):
      continue
    i = line.find( ":" )
    if i < 0: 
      continue
    key   = line[0:i]
    value = line[(i+1):].strip()
    result[ key ] = value
  # Check the data
  if not result.has_key("description"):  
    print "  Missing description!"
    result["description"] = ""
  if not result.has_key("command"):  
    print "  Missing command!"
    result["command"] = "date"
  if result.has_key("id"):
    print "  Extraneous id field: will be ignored and replaced by the filename"
  result["id"] = filename
  if not result.has_key("start_after"):  
    print "  Missing start_after: assuming 00:00"
    result[ "start_after" ] = "00:00"
  if not result.has_key("days"):  
    print "  Missing days: assuming d"
    result[ "days" ] = "d"
  if not result.has_key("depends_on"):  
    result[ "depends_on" ] = ""
  unknown_fields = list( set(result.keys()) - set(["id", "days", "description", "command", "depends_on", "start_after"]) )
  if unknown_fields != []: 
    print "  Unknown fields: ", ", ".join(unknown_fields)
  return result

def read_config_and_initialize_job_table( configuration_directory ):
  print "Jobs previously in table:", get_job_count()
  cursor.execute( "DELETE FROM job" )
  jobs_added = 0
  for dirname, dirnames, filenames in os.walk(configuration_directory):
    print "Processing directory ", dirname
    for file in filenames:
      job = read_file( os.path.join( dirname, file ) )
      add_job( job )
      jobs_added += 1
  connection.commit()
  print "Jobs currently in table:", jobs_added

def check_dependencies(rundate): 
  #print "LOG check_dependencies"
  g = get_dependency_graph()
  for job_id, dependencies in g.iteritems(): 
    for dependency in dependencies:
      #print "LOG check_dependencies", job_id, " ", dependencies
      if not g.has_key( dependency ): 
        print "[PROBLEM] " + job_id + ": unknown dependency " + dependency
        set_job_status( job_id, rundate, INVALID, "Unknown dependency: " + dependency )
  connection.commit()
  # TODO: Look for cycles...
  # (This can be done with a topological sort: find a vertex with no incoming edge, 
  # remove it, iterate -- if you end up with an empty graph, there are no cycles, 
  # if you end up with a graph all of whose vertices have incoming edges, there is a cycle.
  # You can use outgoing edges instead.)

##
## Initialization (for a given day)
##

def initialize_status_table(rundate): 

  # Set the jobs that were running to "unknown" (we do not know if they are still running or not)
  print "Set running jobs to 'unknown'"
  for job in get_running_jobs(rundate):
    print "  Setting to 'unknown':", job
    set_job_status( job, rundate, UNKNOWN, "Was running when server died" )

  # Add an entry for new jobs
  print "New jobs"
  for job_id in get_all_jobs() - get_current_processes(rundate):
    print "  Adding ", job_id
    set_job_status( job_id, rundate, WAITING, "New job added" )

  # Set the jobs that no longer exist to "deleted"
  print "Old jobs"
  for job_id in get_current_processes(rundate) - get_all_jobs():
    print "  Deleting ", job_id
    set_job_status(job_id, rundate, DELETED, "Old job removed" )

  connection.commit()

## Initializations

create_status_table()
create_job_table()
read_config_and_initialize_job_table( configuration )
initialize_status_table( rundate )
check_dependencies( rundate )

## Main loop

running_processes = {}
while True:
  print "Main loop"

  # Look for newly-finished jobs, update their status to done/failed
  print "Checking running processes:", len( running_processes )
  finished_processes = []
  for job, (process,queue) in running_processes.iteritems(): 
    if not process.is_alive(): 
      process.join()
      result = queue.get()
      success = result == 0
      if success: 
        print "  Succeeded:", job
      else:
        print "  Failed:", job
      set_job_status( job, rundate, DONE if success else FAILED, "Return status: " + result.__str__() )
      finished_processes += [ job ]
  for job in finished_processes:
    running_processes.pop( job )
  connection.commit()

  # Update the status of jobs that are waiting
  waiting_jobs = get_jobs_from_status( WAITING, rundate )
  print "Checking jobs in waiting state:", len(waiting_jobs)
  #print "LOG waiting_jobs", get_jobs_from_status( WAITING, rundate )
  for job_id in waiting_jobs:
    #print "LOG waiting_jobs job =", job_id
    job = get_job_description( job_id )
    process = get_process( job_id, rundate )
    #print "LOG waiting_jobs process =", process
    it_is_time = get_timestamp() > get_timestamp( rundate, job["start_after"] ) 
    #print "LOG waiting_jobs wait until:", get_timestamp( rundate, job["start_after"] )
    #print "LOG waiting_jobs now:       ", get_timestamp()
    #print "LOG waiting_jobs it_is_time =", it_is_time
    if not it_is_time:
      print " ", job_id, "start after", get_timestamp( rundate, job["start_after"] ), "now is", get_timestamp()
    dependencies_are_done = True
    waiting_for = set([])
    for dependency in job["depends_on"]:
      #print "LOG waiting_jobs dependency:", dependency
      status = get_process_status(dependency, rundate) 
      if status != DONE:
        dependencies_are_done = False
        waiting_for.add( dependency + " (" + status + ")" )
    if it_is_time and dependencies_are_done:
      print " ", job_id, ": ready"
      set_job_status( job_id, rundate, READY )
    elif not it_is_time:
      print " ", job_id, ": not time yet"
    else:
      print " ", job_id, ": unmet dependencies", waiting_for
  connection.commit()

  # Look for jobs that are ready and launch them
  print "Looking for jobs to launch"
  for job_id in get_jobs_from_status( READY, rundate ):
    command = get_job_description(job_id)[ "command" ]
    print "  Launching", job_id, "command:", command
    log_file = logs + "/" + job_id.replace("/","_").replace(" ","_")
    queue = Queue()
    process = Process( target = run_command, args = (command, log_file, queue,) )
    process.start()
    running_processes[ job_id ] = (process, queue)
    set_job_status( job_id, rundate, RUNNING, log_file )
  connection.commit()

  print "Sleeping"
  connection.commit()
  sleep(5)

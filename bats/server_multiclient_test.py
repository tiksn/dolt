import os
import sys

from queue import Queue
from threading import Thread

from helper.pytest import DoltConnection


# Utility functions

def print_err(e):
    print(e, file=sys.stderr)

def query(dc, query_str):
    return dc.query(query_str, False)

def query_with_expected_error(dc, non_error_msg , query_str):
    try:
        dc.query(query_str, False)
        raise Exception(non_error_msg)
    except:
        pass

def row(pk, c1, c2):
    return {"pk":str(pk),"c1":str(c1),"c2":str(c2)}

UPDATE_BRANCH_FAIL_MSG = "Failed to update branch"

def commit_and_update_branch(dc, commit_message, expected_hashes, branch_name):
    expected_hash = "("
    for i, eh in enumerate(expected_hashes):
        if i != 0:
            expected_hash += " or "
        expected_hash += "hash = %s" % eh
    expected_hash += ")"

    query_str = 'UPDATE dolt_branches SET hash = Commit("%s") WHERE name = "%s" AND %s' % (commit_message, branch_name, expected_hash)
    _, row_count = query(dc, query_str)

    if row_count != 1:
        raise Exception(UPDATE_BRANCH_FAIL_MSG)

    query(dc, 'SET @@repo1_head=HASHOF("%s");' % branch_name)

def query_and_test_results(dc, query_str, expected):
    results, _ = query(dc, query_str)

    if results != expected:
        raise Exception("Unexpected results for query:\n\t%s\nExpected:\n\t%s\nActual:\n\t%s" % (query_str, str(), str(results)))

def resolve_theirs(dc):
    query_str = "REPLACE INTO test (pk, c1, c2) SELECT their_pk, their_c1, their_c2 FROM dolt_conflicts_test WHERE their_pk IS NOT NULL;"
    query(dc, query_str)

    query_str = """DELETE FROM test WHERE pk in (
        SELECT base_pk FROM dolt_conflicts_test WHERE their_pk IS NULL
    );"""
    query(dc, query_str)

    query(dc, "DELETE FROM dolt_conflicts_test")

def create_branch(dc, branch_name):
    query_str = 'INSERT INTO dolt_branches (name, hash) VALUES ("%s", @@repo1_head);' % branch_name
    _, row_count = query(dc, query_str)

    if row_count != 1:
        raise Exception("Failed to create branch")


# work functions

def connect(dc, ctx):
    dc.connect()

def create_tables(dc, ctx):
    query(dc, 'SET @@repo1_head=HASHOF("master");')
    query(dc, """
CREATE TABLE test (
pk INT NOT NULL,
c1 INT,
c2 INT,
PRIMARY KEY(pk));""")
    commit_and_update_branch(dc, "Created tables", ["@@repo1_head"], "master")
    query_and_test_results(dc, "SHOW TABLES;", [{"Table": "test"}])

def duplicate_table_create(dc, ctx):
    query(dc, 'SET @@repo1_head=HASHOF("master");')
    query_with_expected_error(dc, "Should have failed creating duplicate table", """
CREATE TABLE test (
pk INT NOT NULL,
c1 INT,
c2 INT,
PRIMARY KEY(pk));""")


def seed_master(dc, ctx):
    query(dc, 'SET @@repo1_head=HASHOF("master");')
    _, row_count = query(dc, 'INSERT INTO test VALUES (0,0,0),(1,1,1),(2,2,2)')

    if row_count != 3:
        raise Exception("Failed to update rows")

    commit_and_update_branch(dc, "Seeded initial data", ["@@repo1_head"], "master")
    expected = [row(0,0,0), row(1,1,1), row(2,2,2)]
    query_and_test_results(dc, "SELECT pk, c1, c2 FROM test ORDER BY pk", expected)

def race_to_commit1(dc, ctx):
    race_to_commit(dc, ctx, 1, 2)

def race_to_commit2(dc, ctx):
    race_to_commit(dc, ctx, 2, 1)

def race_to_commit(dc, ctx, my_val, their_val):
    query(dc, 'SET @@repo1_head=HASHOF("master");')
    query(dc, "UPDATE test SET c1=%d WHERE pk=0;" % my_val)

    try:
        commit_and_update_branch(dc, "set c1 to 1", ["@@repo1_head"], "master")
        ctx['race_result'] = "winner"

    except Exception as e:
        ctx['race_result'] = "loser"
        create_branch(dc, "merge")
        commit_and_update_branch(dc, "set c1 to 2", "@@repo1_head", "merge")
        query(dc, 'SET @@repo1_head=Merge("master");')
        query_and_test_results(dc, "SELECT * from dolt_conflicts;", [{"table": "test", "num_conflicts": "1"}])
        resolve_theirs(dc)
        expected = [row(0,their_val,0), row(1,1,1), row(2,2,2)]
        query_and_test_results(dc, "SELECT pk, c1, c2 FROM test ORDER BY pk", expected)
        #commit_and_update_branch(dc, "resolved conflicts", 'HASHOF("HEAD~1")', "master")


# test script
MAX_SIMULTANEOUS_CONNECTIONS = 2
PORT_STR = sys.argv[1]

class Session(object):
    def __init__(self):
       self.dc = DoltConnection(port=int(PORT_STR), database="repo1", user='dolt', auto_commit=False)
       self.ctx = {}

SESSIONS = [None]*MAX_SIMULTANEOUS_CONNECTIONS
for i in range(MAX_SIMULTANEOUS_CONNECTIONS):
    SESSIONS[i] = Session()

WORK_QUEUE = Queue()

# work item run by workers
class WorkItem(object):
    def __init__(self, sess, *work_funcs):
        self.session = sess
        self.work_funcs = work_funcs
        self.exception = None


# worker thread function
def worker():
    while True:
        try:
            item = WORK_QUEUE.get()

            for work_func in item.work_funcs:
                work_func(item.session.dc, item.session.ctx)

            WORK_QUEUE.task_done()
        except Exception as e:
            work_item.exception = e
            WORK_QUEUE.task_done()

# start the worker threads
for i in range(MAX_SIMULTANEOUS_CONNECTIONS):
     t = Thread(target=worker)
     t.daemon = True
     t.start()


class Stage(object):
    def __init__(self, work_items, state_validator):
        self.work_items = work_items
        self.state_validator = state_validator

# stage validation
def test_race_results():
    winners = 0
    losers = 0

    for session in SESSIONS:
        if session.ctx['race_result'] == "winner":
            winners += 1
        elif session.ctx['race_result'] == "loser":
            losers += 1

    if winners != 1 and losers != 1:
        raise Exception("concurrency issue. Winners: %d Losers: %d" % (winners, losers))

# This defines the actual test script.  Each stage in the script has a list of work items.  Each work item
# in a stage should have a different connection associated with it.  Each connections work is done in parallel
# each of the work functions for a connection is executed in order.
stages = [
    Stage([WorkItem(SESSIONS[0], connect, create_tables)], None),
    Stage([WorkItem(SESSIONS[0], seed_master), WorkItem(SESSIONS[1], connect, duplicate_table_create)], None),
    Stage([WorkItem(SESSIONS[0], race_to_commit1), WorkItem(SESSIONS[1], race_to_commit2)], test_race_results),
]

# Loop through the work item stages executing each stage by sending the work items for the stage to the worker threads
# and then waiting for all of them to finish before moving on to the next one.  Checks for an error after every stage.
for stage_num, stage in enumerate(stages):
    print("Running stage %d / %d" % (stage_num, len(stages)))
    for work_item in stage.work_items:
        WORK_QUEUE.put(work_item)

    WORK_QUEUE.join()

    for work_item in stage.work_items:
        if work_item.exception is not None:
            print_err(work_item.exception)
            sys.exit(1)

    if stage.state_validator is not None:
        stage.state_validator()

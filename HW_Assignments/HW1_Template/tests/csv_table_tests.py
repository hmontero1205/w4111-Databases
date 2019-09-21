from collections import OrderedDict

from src.CSVDataTable import CSVDataTable
import logging
import os

data_dir = os.path.abspath("../Data/Baseball")

def test_load(db):
    print("****running test_load****")

    print("asserting table and rows are not None...")
    assert(db is not None)
    assert(db.get_rows() is not None)

    print("****test_load succeeded!****\n")

def test_find_by_primary_key(db):
    print("****running test_find_by_primary_key****")

    print("asserting search by primary key only returns 1 result....")
    player_result = db.find_by_primary_key(["aardsda01"])
    assert(len(player_result) == 1)
    print("asserting that offering a larger primary key throws an error")
    try:
        player_result2 = db.find_by_primary_key(["aardsda01", "1981"])
        print("FAILURE: invalid search with two primary keys when only 1 required")
        assert(False)
    except AssertionError:
        assert(False)
    except Exception as e:
        print("error thrown as expected: {}".format(e))
    print("asserting field_list filtering works...")
    player_result3 = db.find_by_primary_key(["aardsda01"], ["nameFirst", "nameLast", "birthCity"])
    assert(len(player_result3) == 1)
    assert(len(player_result3[0].keys()) == 3)

    print("****test_find_by_primary_key succeeded!****\n")

def test_find_by_template(db):
    print("****running test_find_by_template****")

    print("testing template with two fields and asserting values from result match template.....")
    template = {"birthYear":"1990", "birthCountry":"USA"}
    res1 = db.find_by_template(template)
    for p in res1:
        for k, v in template.items():
            assert(p[k] == v)

    print("testing template search with field_list defined and asserting result length same as previous....")
    res2 = db.find_by_template(template, ["birthYear"])
    assert(len(res2) == len(res1))

    print("asserting that non-existent field in field_list throws error....")
    try:
        res3 = db.find_by_template(template, field_list=["personality"])
        print("ERROR: invalid filter by non-existent field")
        assert(False)
    except AssertionError:
        assert(False)
    except Exception as e:
        print("error thrown as expected: {}".format(e))

    print("****test_find_by_template succeeded!****\n")

def test_delete_by_key():
    print("****running test_delete_by_key****")
    connect_info = {
        "directory": data_dir,
        "file_name": "People.csv"
    }
    csv_tbl = CSVDataTable("people", connect_info, ["playerID"])
    before_count = len(csv_tbl.get_rows())
    delete_count = csv_tbl.delete_by_key(["abbeybe01"])
    after_count = len(csv_tbl.get_rows())
    print("asserting valid delete removed 1 row...")
    assert(delete_count == 1)
    assert(before_count == after_count + 1)

    delete_count2 = csv_tbl.delete_by_key(["hjm2133"])
    after_count2 = len(csv_tbl.get_rows())
    print("asserting invalid delete removed 0 rows...")
    assert(delete_count2 == 0)
    assert(after_count == after_count2)

    print("asserting search for deleted primary key returns nothing...")
    empty_query = csv_tbl.find_by_primary_key(["abbeybe01"])
    assert(len(empty_query) == 0)

    print("****test_delete_by_key succeeded!****\n")

def test_delete_by_template():
    print("****running test_delete_find_by_template****")
    connect_info = {
        "directory": data_dir,
        "file_name": "People.csv"
    }
    csv_tbl = CSVDataTable("people", connect_info, ["playerID"])
    before_count = len(csv_tbl.get_rows())
    template = {"playerID":"barbeja01", "birthYear":"1882"}
    delete_count = csv_tbl.delete_by_template(template)
    after_count = len(csv_tbl.get_rows())
    print("asserting valid delete removed correct number of rows...")
    assert(delete_count == 1)
    assert(before_count == after_count + 1)
    print("asserting that query for deleted template returns nothing...")
    empty_query = csv_tbl.find_by_template(template)
    assert(len(empty_query) == 0)

    template2 = {"playerID": "bardda01", "birthYear": "1999"}
    delete_count2 = csv_tbl.delete_by_template(template2)
    after_count2 = len(csv_tbl.get_rows())
    print("asserting invalid delete removed 0 rows...")
    assert(delete_count2 == 0)
    assert(after_count == after_count2)
    print("****test_delete_find_by_template succeeded!****\n")

def test_update_by_key():
    print("****running test_update_by_key****")
    connect_info = {
        "directory": data_dir,
        "file_name": "People.csv"
    }
    csv_tbl = CSVDataTable("people", connect_info, ["playerID"])

    new_vals = {"birthYear":"1999", "nameFirst":"Hans", "playerID":"hjm2133"}
    updated = csv_tbl.update_by_key(["barbest02"], new_vals)
    print("asserting only 1 row updated...")
    assert(updated == 1)
    print("asserting that the row's values were updated....")
    for r in csv_tbl.find_by_primary_key(["barbest02"]):
        for k,v in new_vals.items():
            assert(r[k] == v)

    print("asserting that an update that produces conflicting primary key fails..")
    new_vals2 = {"playerID": "hjm2133", "nameLast": "Montero"}
    try:
        csv_tbl.update_by_key(["abbotda01"], new_vals2)
        print("ERROR: updated another row to have repeated primary key")
        assert (False)
    except AssertionError:
        assert (False)
    except Exception as e:
        print("error thrown as expected: {}".format(e))

    print("****test_update_by_key succeeded!****\n")

def test_update_by_template():
    print("****running test_update_by_template***")
    connect_info = {
        "directory": data_dir,
        "file_name": "People.csv"
    }
    csv_tbl = CSVDataTable("people", connect_info, ["playerID"])

    new_vals = {"birthYear":"1999", "nameFirst":"Hans", "playerID":"hjm2133"}
    template = {"playerID":"abadfe01", "birthCountry":"D.R."}
    updated = csv_tbl.update_by_template(template, new_vals)
    print("asserting correct number of rows updated...")
    assert(updated == 1)
    print("asserting that the row's values were updated....")
    for r in csv_tbl.find_by_template(template):
        for k,v in new_vals.items():
            assert(r[k] == v)

    print("asserting that an update that produces conflicting primary key fails..")
    new_vals2 = {"playerID":"hjm2133", "nameLast":"Montero"}
    template2 = {"birthCountry":"D.R."}
    try:
        csv_tbl.update_by_template(template2, new_vals2)
        print("ERROR: updated several rows to have same primary key")
        assert(False)
    except AssertionError:
        assert(False)
    except Exception as e:
        print("error thrown as expected: {}".format(e))
    print("****test_update_by_template succeeded!****\n")

def test_insert():
    print("****running test_insert***")
    connect_info = {
        "directory": data_dir,
        "file_name": "People.csv"
    }
    csv_tbl = CSVDataTable("people", connect_info, ["playerID"])

    old_row = csv_tbl.find_by_primary_key(["aardsda01"])[0]
    new_row = dict(old_row)
    new_row["playerID"] = "hjm2133"

    before_count = len(csv_tbl.get_rows())
    print("asserting that a new row insert increases row count by 1....")
    csv_tbl.insert(new_row)
    after_count = len(csv_tbl.get_rows())
    assert(after_count == before_count + 1)

    print("asserting that a duplicate row insert does nothing...")
    csv_tbl.insert(new_row)
    after_count2 = len(csv_tbl.get_rows())
    assert(after_count == after_count2)

    print("asserting that attempting to insert row without primary key throws error....")
    no_prim_key = OrderedDict()
    no_prim_key["birthYear"] = "1999"
    no_prim_key["nameFirst"] = "Hans"
    try:
        csv_tbl.insert(no_prim_key)
        print("ERROR: row without primary key inserted!")
        assert(False)
    except AssertionError:
        assert(False)
    except Exception as e:
        print("error thrown as expected: {}".format(e))

    print("asserting that attempting to insert row with conflicting primary key throws error...")
    conf_prim_key = OrderedDict()
    conf_prim_key["playerID"] = "barthji01"
    conf_prim_key["birthYear"] = "1999"
    try:
        csv_tbl.insert(conf_prim_key)
        print("ERROR: row with conflicting primary key inserted!")
        assert(False)
    except AssertionError:
        assert(False)
    except Exception as e:
        print("error thrown as expected: {}".format(e))


    print("****test_insert succeeded!****\n")

def run_tests():
    connect_info = {
        "directory": data_dir,
        "file_name": "People.csv"
    }
    csv_tbl = CSVDataTable("people", connect_info, ["playerID"])
    test_load(csv_tbl)
    test_find_by_primary_key(csv_tbl)
    test_find_by_template(csv_tbl)
    test_delete_by_key()
    test_delete_by_template()
    test_update_by_key()
    test_update_by_template()
    test_insert()

run_tests()
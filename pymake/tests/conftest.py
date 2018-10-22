import os
import shelve
import pytest
import pymake.find_rules
import pymake.makefile
import pymake.tests.rules
from mybuiltins import ason

@pytest.fixture()
def db():
    s = "build/doc_registry.db"

    if os.path.exists(s):
        os.remove(s)

    with shelve.open(s, writeback=True) as db:
        yield db

@pytest.fixture()
def db_meta():
    s = "build/doc_registry_meta.db"
    
    if os.path.exists(s):
        os.remove(s)

    with shelve.open(s, writeback=True) as db_meta:
        yield db_meta


@pytest.fixture()
def makefile(db, db_meta):

    req_cache = None

    m = pymake.makefile.Makefile(req_cache)
    
    #m.rules.append(coil_testing.rules.coiltest.excel_to_text.ExcelToText)
    
    #rules_regex = coil_testing.find_rules.search(coil_testing.rules, pymake.rules.RuleRegex) + \
    #        coil_testing.find_rules.search(coil_testing.rules.fin_surface.tks, pymake.rules.RuleRegex)

   
    #m.rules += rules_regex

    m.add_rules(pymake.find_rules.search(pymake.tests.rules, pymake.rules.RuleDoc))

    m.decoder = ason.Decoder()

    pymake.doc_registry.registry = pymake.doc_registry.DocRegistry(db, db_meta)

    return m









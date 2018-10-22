
import shelve
import pytest
import pymake.makefile
from mybuiltins import ason

@pytest.fixture()
def db():
    with shelve.open("build/doc_registry.db", writeback=True) as db:
        yield db

@pytest.fixture()
def db_meta():
    with shelve.open("build/doc_registry_meta.db", writeback=True) as db_meta:
        yield db_meta


@pytest.fixture()
def makefile():

    req_cache = None

    m = pymake.makefile.Makefile(req_cache)
    
    #m.rules.append(coil_testing.rules.coiltest.excel_to_text.ExcelToText)
    
    #rules_regex = coil_testing.find_rules.search(coil_testing.rules, pymake.rules.RuleRegex) + \
    #        coil_testing.find_rules.search(coil_testing.rules.fin_surface.tks, pymake.rules.RuleRegex)

   
    #m.rules += rules_regex

    m.add_rules(coil_testing.find_rules.search(pymake.tests.rules, pymake.rules.RuleDoc))

    m.decoder = ason.Decoder()

    return m









import time
import os
import shelve
import logging
import logging.config

import pytest

import jelly
import pymake.find_rules
import pymake.makefile
import pymake.tests.rules
import pymake.doc_registry

@pytest.fixture()
def db(event_loop):
    s = "build/doc_registry.db"

    if os.path.exists(s):
        os.remove(s)

    with shelve.open(s, writeback=True) as db:
        yield db

@pytest.fixture()
def db_meta(event_loop):
    s = "build/doc_registry_meta.db"
    
    if os.path.exists(s):
        os.remove(s)

    with shelve.open(s, writeback=True) as db_meta:
        yield db_meta

@pytest.fixture()
def makefile(event_loop, db, db_meta, client):

    
    l = [
            #"%(pathname)30.30s "
            ("%(name){w}.{w}s",     40),
            ("%(funcName){w}s",     23),
            ("%(lineno){w}d",       3),
            ("%(levelname){w}s",    7),
            ("%(message)s",         None),
            ]
    
    FMT_STRING = " ".join([s.format(w=w) for s, w in l])

    LOGGING = {
                    'version': 1,
                    'disable_existing_loggers': False,
                    'handlers': {
                        'console':{
                            'level':'DEBUG',
                            'class':'logging.StreamHandler',
                            'formatter': 'basic'
                            },
                        },
                    'loggers': {
                        'pymake': {
                            'handlers': ['console'],
                            'level': logging.DEBUG,
                            }
                        },
                    'formatters': {"basic": {"format": FMT_STRING}}
                    }
    logging.config.dictConfig(LOGGING)

    req_cache = None

    m = pymake.makefile.Makefile(req_cache)
    
    #m.rules.append(coil_testing.rules.coiltest.excel_to_text.ExcelToText)
    
    #rules_regex = coil_testing.find_rules.search(coil_testing.rules, pymake.rules.RuleRegex) + \
    #        coil_testing.find_rules.search(coil_testing.rules.fin_surface.tks, pymake.rules.RuleRegex)

   
    #m.rules += rules_regex

    m.add_rules(pymake.find_rules.search(pymake.tests.rules, pymake.rules.RuleDoc))

    m.decoder = jelly.Decoder()

    pymake.doc_registry.registry = pymake.doc_registry.DocRegistry(db, db_meta, client)

    return m

@pytest.fixture()
async def client(event_loop):

    s = f'test_{int(time.time())}'

    client = pymake.client.Client(event_loop, s)

    yield client

    await client.drop_database()






import datetime
import os
import time

from SPARQLWrapper import SPARQLWrapper, JSON
from helpers import logger

sparqlQuery = SPARQLWrapper(os.environ.get("MU_SPARQL_ENDPOINT"), returnFormat=JSON)
sparqlUpdate = SPARQLWrapper(os.environ.get("MU_SPARQL_UPDATEPOINT"), returnFormat=JSON)
sparqlUpdate.method = "POST"

def query(the_query, attempt=0, max_retries=5):
    sparqlQuery.setQuery(the_query)
    try:
        start = time.time()
        logger.debug(f"started query at {datetime.datetime.now()}")
        logger.debug("execute query: \n" + the_query)

        return sparqlQuery.query().convert()

        logger.debug(f"query took {time.time() - start} seconds")
    except Exception as e:
        logger.warn(f"Executing query failed unexpectedly. Stacktrace:", e)
        if attempt <= max_retries:
            wait_time = 0.6 * attempt + 30
            logger.warn(f"Retrying after {wait_time} seconds [{attempt}/{max_retries}]")
            time.sleep(wait_time)

            return query(the_query, attempt + 1, max_retries)
        else:
            logger.warn(f"Max attempts reached for query. Skipping.")

def update(the_query, attempt=0, max_retries=5):
    sparqlUpdate.setQuery(the_query)
    if sparqlUpdate.isSparqlUpdateRequest():
        try:
            start = time.time()
            logger.debug(f"started query at {datetime.datetime.now()}")
            logger.debug("execute query: \n" + the_query)

            return sparqlUpdate.query()

            logger.debug(f"query took {time.time() - start} seconds")
        except Exception as e:
            logger.warn(f"Executing query failed unexpectedly. Stacktrace:", e)
            if attempt <= max_retries:
                wait_time = 0.6 * attempt + 30
                logger.warn(f"Retrying after {wait_time} seconds [{attempt}/{max_retries}]")
                time.sleep(wait_time)

                return update(the_query, attempt + 1, max_retries)
            else:
                logger.warn(f"Max attempts reached for query. Skipping.")
    else:
        logger.warn(f"Attempted to execute a non-UPDATE query with the update method. Ignoring query.")

import codecs
import os
import random
import logging
import time

from locust import User, task, between, events, LoadTestShape

import config
from indico_functions import create_client, submit_indico_request

INDICO_CLIENT = create_client(config.HOST, config.API_TOKEN_PATH)
PDF_DIR = os.getcwd() + "/pdfs"
PDF_FILES = os.listdir(PDF_DIR)


class IndicoTaskSet(User):

    @task(1)
    def request_submission(self):
        start_time = time.time()
        pdf_name = random.choice(PDF_FILES)
        pdf_stream = codecs.open(filename=PDF_DIR + "/" + pdf_name, mode="rb")
        submission_ids = []
        try:
            submission_ids = submit_indico_request(INDICO_CLIENT, config.INDICO_WORKFLOW_ID, pdf_name, pdf_stream)
            events.request_success.fire(
                request_type="sdk",
                name="request_submission",
                response_time=(time.time() - start_time) * 1000,
                response_length=1
            )
        except Exception as ex:
            events.request_failure.fire(
                request_type="sdk",
                name="request_submission",
                response_time=(time.time() - start_time) * 1000,
                response_length=1
            )
            raise ex
        submission_id = str(submission_ids).strip("[]")
        logging.info("submission_id: " + submission_id + ", pdf_name: " + pdf_name)


class IndicoUser(User):
    tasks = [IndicoTaskSet]

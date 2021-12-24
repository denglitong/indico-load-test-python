import time
from io import BytesIO

from indico import (
    IndicoClient,
    IndicoConfig,
)

from indico.queries import (
    RetrieveStorageObject,
    # ModelGroupPredict,
    # JobStatus,
    # DocumentExtraction,
    CreateDataset,
    AddFiles,
    ProcessCSV,
    ListDatasets,
    CreateModelGroup,
    GraphQLRequest,
    WorkflowSubmission,
    SubmissionResult,
    GetSubmission
)

import config
# import json


def create_client(host, api_token_path):
    my_config = IndicoConfig(host=host, api_token_path=api_token_path)
    client = IndicoClient(config=my_config)
    return client


def create_dataset(client, dataset_name, dataset_csv):
    # Creates a new indico dataset with {dataset_name} if it does not exist. Indico create_dataset requires a csv

    dataset_list = client.call(
        ListDatasets(
        )
    )

    dataset_ids = [
        f.id for f in dataset_list if f.name == dataset_name
    ]

    # check if dataset already exists
    if not dataset_ids:
        dataset = client.call(
            CreateDataset(
                name=dataset_name,
                files=[dataset_csv],
                dataset_type="TEXT",
                wait=True,
            )
        )

    # if dataset already exists, upload csv to the dataset then process the csv
    else:
        dataset = client.call(
            AddFiles(
                dataset_id=dataset_ids[0],
                files=[dataset_csv],
                wait=True,
                batch_size=1,
            )
        )

        dataset = process_dataset(client, dataset)

    return dataset


def add_csv_file(client, dataset, dataset_csv):
    # Adds csv file to a dataset

    dataset = client.call(
        AddFiles(
            dataset_id=dataset.id,
            files=[dataset_csv],
            wait=True,
            batch_size=1,
        )
    )

    return dataset


def process_dataset(client, dataset):
    # Process csv files in a dataset

    datafile_ids = [
        f.id for f in dataset.files if f.status == "DOWNLOADED"
    ]

    dataset = client.call(
        ProcessCSV(
            dataset_id=dataset.id,
            datafile_ids=datafile_ids,
            wait=True
        )
    )

    return dataset


def create_model(client, dataset, source_column_name, label_column_name, model_name):
    # create and train the model w/ the relevant csv columns

    model_group = client.call(
        CreateModelGroup(
            name=model_name,
            dataset_id=dataset.id,
            source_column_id=dataset.datacolumn_by_name(source_column_name).id,  # csv text column
            labelset_id=dataset.labelset_by_name(label_column_name).id,  # csv target class column
            wait=True,  # wait for training to finish
        )
    )

    return model_group


def retrain_model(client, model_id):
    # retrain train the model

    query = """
        mutation retrainMoonbowModelGroup($modelGroupId: Int!) {
            retrainModelGroup(modelGroupId: $modelGroupId, forceRetrain: true) {
                id
                status
                __typename
            }
        }
    """
    variables = {'modelGroupId' : model_id}

    model_group = client.call(
        GraphQLRequest(
            query,
            variables
        )
    )

    return model_group


def submit_indico_request(client, workflow_id, filename, files_to_upload):

    if files_to_upload is not list:
        submission_ids = client.call(WorkflowSubmission(workflow_id, streams={filename: files_to_upload}))
    else:
        upload_dict = {}
        page_cnt = 1
        for file in files_to_upload:
            file_pair = {str(filename) + str(page_cnt): BytesIO(file)}
            upload_dict.update(file_pair)
            page_cnt = page_cnt + 1
        submission_ids = client.call(WorkflowSubmission(workflow_id, streams=upload_dict))

    return submission_ids


def retrieve_indico_result(client, submission_id):

    # submission_results = client.call(SubmissionResult(submission_id, wait=True))
    # results = client.call(RetrieveStorageObject(submission_results.result))

    sub = client.call(GetSubmission(submission_id))
    results = client.call(RetrieveStorageObject(sub.result_file))

    return results


if __name__ == "__main__":
    # testing indico_functions

    import aws_functions
    import os

    tempclient = create_client(config.HOST, config.API_TOKEN_PATH)
    #
    # result = retrieve_indico_result(tempclient, 31)
    #
    # print(result)



    # bucket_name = "beta-indico-document-directory"
    #
    # file_key = "uploads/test.pdf"
    #
    # s3_file_stream = aws_functions.get_s3_pdf(bucket_name, file_key)
    #
    # submission_id = submit_indico_request(tempclient, 4, os.path.basename(file_key), s3_file_stream)
    # print(submission_id)

    result = retrieve_indico_result(tempclient, 15)

    final_result = []
    for item in result:
        final_result.append({'start_page': str(int(item['start_page'])+1), 'end_page': item['end_page'], 'label': item['label'], 'confidence': item ['confidence']})
    print(final_result)

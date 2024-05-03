#!/usr/bin/python3

import os
import time
import json

# Required libraries (need installing)
import requests


class FlowGraphEngineServer():
    """Autodesk Platform Services (APS) Flow Server interface encapsulation class

    Reference:
        * APS: https://github.com/autodesk-platform-services
        * Flow Graph Engine: https://aps.autodesk.com/en/docs/flow_graph_engine/v1/
        * Bifrost Nodes: https://help.autodesk.com/view/BIFROST/ENU/?guid=Bifrost_Common_reference_html
        * Sample: https://github.com/autodesk-platform-services/aps-flowgraphengine-bifrost-js-sample

    Sample Usage:
        fs = FlowGraphEngineServer(...)
        fs.upload_file(...)
        fs.submit_job(...)
        fs.wait_for_job_to_complete(...)
        fs.download_job_logs(...)
        fs.download_job_outputs(...)

    Notes:
        For storage_space, input files should go to 'scratch:@default'
            Currently 'scratch:@default' is the only one supported for input files. 
            (scratch=storage provider, and @default is the storage space)
    """
    def __init__(
            self, 
            client_id, 
            client_secret,
            queue_id='@default',
            ):
        """
        Parameters:
            client_id         APS Client ID
            client_secret     APS Client Secret
            queue_id          Job Queue ID (default='@default' for personal queue)
        """
        self.client_id        = client_id
        self.client_secret    = client_secret
        assert(self.client_id)
        assert(self.client_secret)
        self.oauth_token = self._generate_oauth_token()
        self.queue_id         = queue_id


    def _generate_oauth_token(self):
        """Validate Client Credentials (ID, Secret) and return oauth_token

        Requires:
            self.client_id
            self.client_secret
        """
        response = requests.post(
            'https://developer.api.autodesk.com/authentication/v2/token', {
                'scope'        : 'data:read data:create data:write code:all',
                'grant_type'   : 'client_credentials',
                'client_id'    : self.client_id,
                'client_secret': self.client_secret,
            },
            headers={
                'Content-Type': 'application/x-www-form-urlencoded',
            }
        )
        return response.json()['access_token']


    def _get_resource_upload_url(self, space_id, resource_id):
        response = requests.get(
            f'https://developer.api.autodesk.com/flow/storage/v1/spaces/{space_id}/resources/{resource_id}/upload-urls',
            headers={'Authorization': f'Bearer {self.oauth_token}'})
        return response.json()


    def _upload_to_signed_url(self, signedUrl, path_to_file):
        with open(path_to_file, 'rb') as f:
            response = requests.put(signedUrl, data=f.read())
        return response.headers['etag']


    def _complete_upload(self, space_id, resource_id, upload_id, etag):
        url = f'https://developer.api.autodesk.com/flow/storage/v1/spaces/{space_id}/uploads:complete'
        json_data = {
            'resourceId': resource_id,
            'uploadId'  : upload_id,
            'parts': [{
                    'partId': 1,
                    'etag': etag
                }]
            }
        headers = {'Authorization': f'Bearer {self.oauth_token}'}
        response = requests.post(url, json=json_data, headers=headers)
        return response.json()['urn']


    def upload_file(self, path_to_file, space_id, resource_id):
        getInputFileUploadUrlResponse = self._get_resource_upload_url(space_id, resource_id)
        inputFileEtag = self._upload_to_signed_url(getInputFileUploadUrlResponse['urls'][0]['url'], path_to_file)
        inputFileUrn = self._complete_upload(space_id, getInputFileUploadUrlResponse['upload']['resourceId'], getInputFileUploadUrlResponse['upload']['id'], inputFileEtag)
        return inputFileUrn


    def list_jobs(self):
        response = requests.get(
            f'https://developer.api.autodesk.com/flow/compute/v1/queues/{self.queue_id}/jobs',
            headers={'Authorization': f"Bearer {self.oauth_token}"})
        return response.json()['results']


    def submit_job(self, job_json):
        """
        Reference:
            * https://aps.autodesk.com/en/docs/flow_graph_engine/v1/reference/quick_reference/job-createjob-POST/
            * https://aps.autodesk.com/en/docs/flow_graph_engine/v1/reference/executor_payload/bifrost_payload/
        """
        response = requests.post(
            f"https://developer.api.autodesk.com/flow/compute/v1/queues/{self.queue_id}/jobs",
            json=job_json,
            headers={'Authorization': f"Bearer {self.oauth_token}"})
        return response.json()['id']


    def get_job_data(self, job_id):
        response = requests.get(
            f'https://developer.api.autodesk.com/flow/compute/v1/queues/{self.queue_id}/jobs/{job_id}',
            headers={'Authorization': f"Bearer {self.oauth_token}"})
        return response.json()


    def wait_for_job_to_complete(self, job_id, sleep_seconds=5):
        while True:
            job = self.get_job_data(job_id)
            print(f"Job status: {job['status']}")
            if job['status'] in ('SUCCEEDED', 'FAILED', 'CANCELED'):
                break
            time.sleep(sleep_seconds)
        return job


    def _get_log_data(self, job_id):
        response = requests.get(
            f"https://developer.api.autodesk.com/flow/compute/v1/queues/{self.queue_id}/jobs/{job_id}/logs",
            headers={'Authorization': f"Bearer {self.oauth_token}"})
        return response.json()


    def _get_output_data(self, job_id):
        response = requests.get(
            f"https://developer.api.autodesk.com/flow/compute/v1/queues/{self.queue_id}/jobs/{job_id}/outputs",
            headers={'Authorization': f"Bearer {self.oauth_token}"})
        return response.json()


    def _get_download_url_for_resource(self, space_id, resourceId):
        response = requests.get(
            f"https://developer.api.autodesk.com/flow/storage/v1/spaces/{space_id}/resources/{resourceId}/download-url",
            headers={'Authorization': f"Bearer {self.oauth_token}"})
        return response.json()


    def _download_file_from_signed_url(self, signedUrl, destination, chunk_size=8192):
        response = requests.get(signedUrl, stream=True)
        if response.status_code == 200:
            with open(destination, 'wb') as f:
                for chunk in response.iter_content(chunk_size=chunk_size):
                    f.write(chunk)


    def download_file(self, space_id, resource_id, destination_path):
        signedUrl = self._get_download_url_for_resource(space_id, resource_id)
        self._download_file_from_signed_url(signedUrl['url'], destination_path)


    def download_job_logs(self, job_id, logdir='.'):
        logs = self._get_log_data(job_id)
        log_filepaths = []
        for log in logs['results']:
            log_filepath = f"{logdir}/joblog_{os.path.basename(log['path'])}"
            downloadUrl = self.download_file(log['spaceId'], log['resourceId'], log_filepath)
            log_filepaths.append(log_filepath)
        return log_filepaths


    def download_job_outputs(self, job_id, outdir='.'):
        # Downloading outputs for the job
        outputs = self._get_output_data(job_id)
        output_filepaths = []
        for output in outputs['results']:
            output_filepath = f"{outdir}/{os.path.basename(output['path'])}"
            downloadUrl = self.download_file(output['spaceId'], output['resourceId'], output_filepath)
            output_filepaths.append(output_filepath)
        return output_filepaths

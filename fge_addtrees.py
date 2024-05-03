#!/usr/bin/python3

import os
import argparse
import flow_graphengine


def main(
        aps_client_id     : str,
        aps_client_secret : str,
        number_of_trees   : int = 1000,
        **unused_kwargs):

    # Init Server Connection
    fs = flow_graphengine.FlowGraphEngineServer(
        client_id=aps_client_id,
        client_secret=aps_client_secret,
        queue_id='@default')

    # Upload files
    print('Uploading input files')
    input_dir       = f'{os.path.dirname(__file__)}/input-data'
    inputFileUrn    = fs.upload_file(f'{input_dir}/plane.usd'    , 'scratch:@default', 'plane.usd')
    bifrostGraphUrn = fs.upload_file(f'{input_dir}/addTrees.json', 'scratch:@default', 'addTrees.json')

    # Submit job
    job_json = {
        'name': 'addTrees sample job',
        'tags': ['sample app'],
        'tasks': [
            {
                'name'    : 'execute bifrost graph',
                'type'    : 'task',
                'executor': 'bifrost',
                'inputs'  : [],
                'limitations': {
                    'maxExecutionTimeInSeconds': 600,
                },
                'payload': {
                    'action': 'Evaluate',
                    'options': {
                        'compound': 'User::Graphs::addTrees',
                        'frames': {
                            'start': 1,
                            'end'  : 1,
                        }
                    },
                    'definitionFiles': [{
                            'source': {
                                'uri': bifrostGraphUrn
                            },
                            'target': {
                                'path': 'bifrostgraph.json'
                            },
                        }
                    ],
                    'ports':{
                        'inputPorts': [
                            {
                                'name' : 'inputFilename',
                                'value': 'plane.usd',
                                'type' : 'string',
                            },
                            {
                                'name' : 'outputFilename',
                                'value': 'planeWithTrees.usd',
                                'type' : 'string',
                            },
                            {
                                'name' : 'amount',
                                'value': str(number_of_trees),
                                'type' : 'float',
                            }
                        ],
                        'jobPorts': [],
                    },
                    'executions': [
                        {
                            'inputs': [
                                {
                                    'source': {
                                        'uri': inputFileUrn,
                                    },
                                    'target': {
                                        'path': 'plane.usd',
                                    }
                                },
                            ],
                            'outputs': [
                                {
                                    'source': {
                                        'path': 'planeWithTrees.usd',
                                    },
                                    'target': {
                                        'name': 'planeWithTrees.usd',
                                    }
                                }
                            ],
                            'frameId': 1,
                        }
                    ],
                },
                'requirements': {
                    'cpu': 4,
                    'memory': 30720,
                }
            }
        ]
    }

    # Submit job
    job_id = fs.submit_job(job_json)
    print(f"Job submitted, id: {job_id}")

    # List jobs
    joblist = fs.list_jobs()
    print(f'Joblist Queue (all): {len(joblist)}')

    print('waiting for job to complete')
    job = fs.wait_for_job_to_complete(job_id, sleep_seconds=5)
    print(f'job finished with status {job["status"]}')
    
    # Get Results
    outdir = '.outputs'
    os.makedirs(outdir, exist_ok=True)

    print('Downloading job outputs')
    filenames = fs.download_job_outputs(job_id, outdir=outdir)
    print(f'Downloaded job outputs ({len(filenames)}): {filenames}')

    logdir = '.logs'
    os.makedirs(logdir, exist_ok=True)
    print('Downloading job logs')
    filenames = fs.download_job_logs(job_id, logdir=logdir)
    print(f'Downloaded job logs ({len(filenames)}): {filenames}')


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="""Sample APS Flow GraphEngine Tree Scatter code""")
    # General
    group = parser.add_argument_group('general')

    group = parser.add_argument_group('authentication')
    group.add_argument('-ci', '--client-id', type=str,
                       dest='aps_client_id',
                       default=os.environ.get("APS_CLIENT_ID"),
                       help='client-id for APS authentication. Alternate: Use envvar APS_CLIENT_ID',
                       metavar='STR')
    group.add_argument('-cs', '--client-secret', type=str,
                       dest='aps_client_secret',
                       default=os.environ.get("APS_CLIENT_SECRET"),
                       help='client-secret for APS authentication. Alternate: Use envvar APS_CLIENT_SECRET',
                       metavar='STR')

    group = parser.add_argument_group('configuration')
    group.add_argument('--trees', type=str,
                       dest='number_of_trees',
                       default=100,
                       help='Number of trees to scatter',
                       metavar='INT')
    args = parser.parse_args()

    # Call main with args
    args_dict = vars(args)
    main(**args_dict)


import sys
import argparse
import requests
import shipyard_utils as shipyard


EXIT_CODE_SYNC_STATUS_SUCCESS = 0
EXIT_CODE_BAD_REQUEST = 201
EXIT_CODE_SYNC_STATUS_ERROR = 210
EXIT_CODE_SYNC_STATUS_INCOMPLETE = 211


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--access-token', dest='access_token', required=True)
    parser.add_argument('--source-id', dest='source_id', required=False)
    args = parser.parse_args()
    return args


def get_source_status(source_id, access_token):
    api_headers = {
        'authorization': f"Bearer {access_token}",
        'Content-Type': 'application/json'
    }
    api_url = "https://api.rudderstack.com/v2/sources"
    source_status_url = api_url + f"/{source_id}/status"
    source_status_json = {}
    status_code = EXIT_CODE_SYNC_STATUS_SUCCESS
    
    # get response from API
    try:
        source_status_response = requests.get(source_status_url, 
                                           headers=api_headers)
        # check if successful, if not return error message
        if source_status_response.status_code == requests.codes.ok:
            source_status_json = source_status_response.json()
        else:
            print(f"Source status check failed. Reason: {source_status_response.content}")
            sys.exit(EXIT_CODE_BAD_REQUEST)
    except Exception as e:
        print(f"Source {source_id} status check failed due to: {e}")
        sys.exit(EXIT_CODE_BAD_REQUEST)
    
    # check source sync status
    if source_status_json['status'] == 'finished':
        print("Successfully managed to check sync")
        if source_status_json.get('error'):
            print(f"sync for sourceId: {source_id} failed with error: {source_status_json['error']}")
            sys.exit(EXIT_CODE_SYNC_STATUS_ERROR)
    else:
        print(f"Sync for {source_id} is incomplete. Status {source_status_json['status']}")
        status_code = EXIT_CODE_SYNC_STATUS_INCOMPLETE
        
    return status_code
    
    
def main():
    args = get_args()
    access_token = args.access_token  
    # create artifacts folder to save run id
    base_folder_name = shipyard.logs.determine_base_artifact_folder(
        'rudderstack')
    artifact_subfolder_paths = shipyard.logs.determine_artifact_subfolders(
        base_folder_name)
    shipyard.logs.create_artifacts_folders(artifact_subfolder_paths)
    
    # get Source id variable from user or pickle file if not inputted
    if args.source_id:
        source_id = args.source_id
    else:
        source_id = shipyard.logs.read_pickle_file(
            artifact_subfolder_paths, 'source_id')
        
    # run check sync status
    exit_code_status = get_source_status(source_id, access_token)
    sys.exit(exit_code_status)


if __name__ == "__main__":
    main()
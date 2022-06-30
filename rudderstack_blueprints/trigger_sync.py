import argparse
import sys
import requests
import shipyard_utils as shipyard

try:
    import errors
except BaseException:
    from . import errors


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--access-token', dest='access_token', required=True)
    parser.add_argument('--source-id', dest='source_id', required=True)
    args = parser.parse_args()
    return args


def trigger_sync(source_id, access_token):
    api_headers = {
        'authorization': f"Bearer {access_token}",
        'Content-Type': 'application/json'
    }
    api_url = "https://api.rudderstack.com/v2/sources"
    trigger_sync_url = api_url + f"/{source_id}/start"
    # get response from API
    try:
        trigger_sync_response = requests.post(trigger_sync_url, 
                                           headers=api_headers)
        # check if successful, if not return error message
        if trigger_sync_response.status_code  in (200, 204, 201):
            print(f"Trigger sync for Source ID: {source_id} successful")

        elif trigger_sync_response.status_code == 409:
            print(f"Sync job for Source ID: {source_id} is already running")
            sys.exit(errors.EXIT_CODE_SYNC_ALREADY_RUNNING)

        elif trigger_sync_response.status_code == 401:
            print(f"Trigger sync failed for Source {source_id} due to Invalid Credentials")
            sys.exit(errors.EXIT_CODE_INVALID_CREDENTIALS)

        elif trigger_sync_response.status_code == 500:
            print(f"Trigger sync failed. Check if Source {source_id} exists")
            sys.exit(errors.EXIT_CODE_SYNC_INVALID_SOURCE_ID)

        else:
            print(f"Trigger sync failed. Error code:{trigger_sync_response.status_code}")
            sys.exit(errors.EXIT_CODE_BAD_REQUEST)
    except Exception as e:
        print(f"Source {source_id} trigger sync failed due to: {e}")
        sys.exit(errors.EXIT_CODE_UNKNOWN_ERROR)


def main():
    args = get_args()
    access_token = args.access_token
    source_id = args.source_id
    
    # execute trigger sync
    trigger_sync(source_id, access_token)
    
    # create artifacts folder to save run id
    base_folder_name = shipyard.logs.determine_base_artifact_folder(
        'rudderstack')
    artifact_subfolder_paths = shipyard.logs.determine_artifact_subfolders(
        base_folder_name)
    shipyard.logs.create_artifacts_folders(artifact_subfolder_paths)
    
    # save sync run id as variable
    shipyard.logs.create_pickle_file(artifact_subfolder_paths, 
                                'source_id', source_id)


if __name__ == "__main__":
    main()
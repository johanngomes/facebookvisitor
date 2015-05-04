__author__ = 'johann'

import json

class ConfReader:
    file = open('conf/conf.json')
    data = json.load(file)
    file.close()

    RUNNING_MODE = data["visitor"]["running_mode"]
    MAX_DAYS_SPENT_TO_CRAWL = data["visitor"]["max_days_spent_to_crawl"]
    VISIT_INCREMENT = data["visitor"]["visit_increment"]
    REVISIT_INCREMENT = data["visitor"]["revisit_increment"]
    ROOT_FEED_ID = data["visitor"]["root_feed_id"]
    VISIT_MULTIPLE = data["visitor"]["visit_multiple"]
    TIMEOUT_SECONDS = data["visitor"]["timeout_seconds"]

    AUTHENTICATION_DATA_PATH = data["facebook_authenticator"]["authentication_data_path"]
    SAVED_ACCESS_TOKEN_PATH = data["facebook_authenticator"]["saved_access_token_path"]

    SOURCES_PER_SET = data["batch"]["sources_per_set"]

    TRY_AGAIN_TIMEOUT = data["utc"]["http_try_again_timeout"]

    PORTUGUESE_GUESSED_PERCENTAGE = data["feed_filter"]["portuguese_guessed_percentage"]

if __name__ == '__main__':
    print ConfReader.AUTHENTICATION_DATA_PATH

# instantiate PS client (load config YAML)

# check if auth token exists (google secret)
    # if exists: check token validity
        # if not valid: generate new auth token
            # save auth token (google secret)
    # if not existing: generate new auth token
        # save auth token (google secret)

# load query config (YAML)

# for each query:
    # parse params
    # get table count
    # get table data
    # save to GCS (resource?)
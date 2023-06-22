# jstor_aspace_publisher

### To start component:
- clone repository
- cp .env.example to .env
- cp celeryconfig.py.example to celeryconfig.py and put in credentials
- make sure logs/jstor_publisher directory exists (need to fix)
- bring up docker
- - docker-compose -f docker-compose-local.yml up --build -d --force-recreate


### Generate delete file:
- start docker (above)
- exec into container
- - docker exec -it <containerid> bash
- - > python3 scripts/generate_deletes.py <YYYY> <MM> <DD>
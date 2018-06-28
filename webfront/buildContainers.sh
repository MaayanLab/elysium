# get rid of old stuff
docker rmi -f $(docker images | grep "^<none>" | awk "{print $3}")
docker rm $(docker ps -q -f status=exited)

docker kill cloudalignment
docker rm cloudalignment


docker build -f Dockerfile -t maayanlab/cloudalignment .


docker push maayanlab/cloudalignment

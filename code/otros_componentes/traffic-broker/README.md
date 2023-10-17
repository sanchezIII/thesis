## Building the image 
```
docker build -t traffic-broker .
```

## Running the image in a container
```
docker run -d --name traffic-broker -p 7000:7000 traffic-broker
```
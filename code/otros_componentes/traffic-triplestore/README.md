## Pull the image
```
docker pull stardog/stardog:latest
```
## Building the image
```
docker build -t traffic-triplestore .
```

## Running the image in a container
```
docker run -d --name traffic-triplestore -p 5820:5820 traffic-triplestore
```

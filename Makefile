build: Dockerfile
	DOCKER_BUILDKIT=1 docker build -f Dockerfile --no-cache $(BUILD_ARGS) . --target exporter --output ./


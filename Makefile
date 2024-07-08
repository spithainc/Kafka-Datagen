build: Dockerfile.raw
	DOCKER_BUILDKIT=1 docker build -f Dockerfile.raw --no-cache $(BUILD_ARGS) . --target exporter --output ./


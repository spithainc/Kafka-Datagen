## common values
VERSION ?= $(shell cat ../VERSION)
TARGET := $(shell cat ./TARGET)

## build arg
BUILD_ARGS := --no-cache --force-rm 
BUILD_ARGS += --build-arg VERSION=$(VERSION)

DOCKER = docker

clean:
	$(DOCKER) rmi -f "$(TARGET):latest"
	$(DOCKER) rmi -f "$(TARGET):$(VERSION)"
	rm -rf dist load_docker-image.sh ./$(TARGET)-docker.tar ./src

copy-source-files:
	rm -rf src && mkdir src && rsync -av --exclude='docker' --exclude='raw' ../ src/

docker: copy-source-files
	DOCKER_BUILDKIT=1 $(DOCKER) build \
	$(BUILD_ARGS) \
	-t "$(TARGET):latest" \
	-t "$(TARGET):$(VERSION)" \
	./

$(TARGET)-docker.tar: docker
	docker save $(TARGET):$(VERSION) > ./$(TARGET)-docker.tar

dist: $(TARGET)-docker.tar
	mkdir -p dist
	TARGET=$(TARGET) VERSION=$(VERSION) envsubst '$${TARGET},$${VERSION}' < ./load_docker-image.tmpl > ./load_docker-image.sh
	chmod a+x load_docker-image.sh
	tar zcf dist/$(TARGET)-docker_$(VERSION).tar.gz \
	--transform 's,^,$(TARGET)-docker_$(VERSION)/,' \
	./$(TARGET)-docker.tar ./load_docker-image.sh



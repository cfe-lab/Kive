
# Makefile for kivebackup image .. run make to see a menu
BASE_IMAGE_NAME = kivebackup-base
BASE_DOCKERFILE = kivebackup-base.dock

#NOTE: there may be no leading or trailing spaces on the R.H.S, or the -v option will fail
DEV_SRC          =kivebackup-devel/kivebackupsrc

#NOTE: docker requires an absolute name here -- we assume the home directory,
# but change this appropriately in order to run the docker image
KIVEBACKUP_TEST_DIR =${HOME}/kivebackup-devel/kivebackup-testdir


#ENV_OPTS = -ePYTHONPATH=/kivebackupsrc/utils
ENV_OPTS = 

default: help

# NOTE: this code taken from https://gist.github.com/rcmachado/af3db315e31383502660
help: 
	$(info Available targets:)
	@awk '/^[a-zA-Z\-\_0-9]+:/ {                                   \
          nb = sub( /^## /, "", helpMsg );                             \
          if(nb == 0) {                                                \
            helpMsg = $$0;                                             \
            nb = sub( /^[^:]*:.* ## /, "", helpMsg );                  \
          }                                                            \
          if (nb)                                                      \
            printf "\033[1;31m%-" width "s\033[0m %s\n", $$1, helpMsg; \
        }                                                              \
        { helpMsg = $$0 }'                                             \
        width=$$(grep -o '^[a-zA-Z_0-9]\+:' $(MAKEFILE_LIST) | wc -L)  \
	$(MAKEFILE_LIST)


build: $(BASE_DOCKERFILE) ## build the docker image for kivebackup
	( docker build  -f $(BASE_DOCKERFILE) -t ${BASE_IMAGE_NAME} . )

build-nocache: $(BASE_DOCKERFILE) ## build the docker image for kivebackup from scratch
	( docker build --no-cache -f $(BASE_DOCKERFILE) -t ${BASE_IMAGE_NAME} . )

run:  ## run the docker image for kivebackup interactively with tcsh
	docker run  --rm -v ${KIVEBACKUP_TEST_DIR}:/kivebackup  -it --net="host" ${BASE_IMAGE_NAME} tcsh

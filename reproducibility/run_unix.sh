#!/bin/bash

export DOCKER_CLI_HINTS=false

# checks if docker exists. exits if not
check_docker() {
    if ! command -v docker 2>&1 >/dev/null; then
        echo "docker not found"
        exit 1
    fi

    if ! docker ps > /dev/null 2>&1; then
        echo "docker was found but cannot be executed (missing sudo?)"
        exit 2
    fi
}

# builds the main image
build() {
    echo "Building the main image"
    chmod +x scripts/*.sh
    cd ..
    docker build . -t crdv-reproducibility -f reproducibility/containers/Dockerfile.main -q > /dev/null
    cd reproducibility
}

# starts the main container if exists, otherwise creates a new one
run_container() {
    echo "Starting the main container"
    docker start crdv-reproducibility &> /dev/null || docker run --name crdv-reproducibility \
        -v ./results:/main/reproducibility/results \
        -v ./conf:/main/reproducibility/conf \
        -v ./document:/main/reproducibility/document \
        -dit --privileged crdv-reproducibility > /dev/null

    echo "Waiting for the docker service to start"
    while [ "$(docker inspect -f {{.State.Health.Status}} crdv-reproducibility)" != "healthy" ]; do
        sleep 1
    done;
}

# removes the main container and image
cleanup() {
    echo "Deleting the main container and image"
    docker rm --force crdv-reproducibility > /dev/null
    docker rmi crdv-reproducibility > /dev/null
}

main() {
    check_docker
    build
    run_container

    if [ $# -eq 0 ]; then
        echo "Running all tests"
        docker exec -it crdv-reproducibility ./run.sh all
        cleanup
    else
        clean=false
        for arg in "$@"; do
            # save the cleanup to only do it at the end
            if [ "$arg" == "cleanup" ]; then
                clean=true
            else
                docker exec -it crdv-reproducibility ./run.sh "$arg"
            fi
        done

        if [ "$clean" = true ]; then
            cleanup
        fi
    fi
}

main "$@"

# checks if docker exists. exits if not
function check_docker() {
    if (-not(Get-Command docker -errorAction SilentlyContinue)) {
        echo "docker not found"
        exit 1
    }
}

# builds the main image
function build() {
    echo "Building the main image"
    cd ..
    docker build . -t crdv-reproducibility -f reproducibility/containers/Dockerfile.main -q | out-null
    cd reproducibility
}

# starts the main container if exists, otherwise creates a new one
function run_container() {
    echo "Starting the main container"
    docker start crdv-reproducibility 2>&1 | out-null || docker run --name crdv-reproducibility `
        -v ${pwd}/results:/main/reproducibility/results `
        -v ${pwd}/conf:/main/reproducibility/conf `
        -v ${pwd}/document:/main/reproducibility/document `
        -dit --privileged crdv-reproducibility | out-null

    echo "Waiting for the docker service to start"
    while ((docker inspect -f '{{.State.Health.Status}}' crdv-reproducibility) -ne 'healthy') {
        Start-Sleep -Seconds 1
    }
}

# removes the main container and image
function cleanup() {
    echo "Deleting the main container and image"
    docker rm --force crdv-reproducibility | out-null
    docker rmi crdv-reproducibility | out-null
}

function main($tests) {
    check_docker
    build
    run_container

    if ($tests.Count -eq 0) {
        echo "Running all tests"
        docker exec -t crdv-reproducibility ./run.sh all
        cleanup
    } else {
        $clean = $false
        foreach ($test in $tests) {
            # save the cleanup to only do it at the end
            if ($test -eq "cleanup") {
                $clean = $true
            } else {
                docker exec -t crdv-reproducibility ./run.sh "$test"
            }
        }

        if ($clean -eq $true) {
            cleanup
        }
    }
}

main $args

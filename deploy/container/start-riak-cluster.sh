docker-compose -f docker-compose-riak.yml up -d

N=10

for ((i = 0; i < $N; i++)); do
    docker exec container-riak$i-1 bash -c "cd deploy/riak; ./run.sh 1; ./create-bucket-types.sh 1; ./repl-prepare.sh C$i" &
done

wait

connect() {
    for ((j = 0; j < $N; j++)); do
        if [ $1 -ne $j ]; then
            docker exec container-riak$1-1 bash -c "cd deploy/riak; ./repl-connect.sh C$j riak$j"
        fi
    done
}

for ((i = 0; i < $N; i++)); do
    connect $i &
done

wait

sleep 10

for ((i = 0; i < $N; i++)); do
    docker exec container-riak$i-1 bash -c "cd deploy/riak; ./repl-connections.sh"
done

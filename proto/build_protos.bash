set -e
export TMPGOPATH=`pwd`/../../../..
protoc --go_out=plugins=grpc:$TMPGOPATH/src/ --proto_path=$TMPGOPATH/src $TMPGOPATH/src/github.com/runningwild/jig/proto/jig.proto
go install github.com/runningwild/jig/proto

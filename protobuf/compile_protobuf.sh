cd $(dirname $0)
# Instructions adopted from https://planspace.org/20170329-protocol_buffers_in_python/
curl https://developers.google.com/transit/gtfs-realtime/gtfs-realtime.proto -o gtfs-realtime.proto
curl http://datamine.mta.info/sites/all/files/pdfs/nyct-subway.proto.txt -o nyct-subway.proto

python -m grpc_tools.protoc --proto_path=./ --python_out=../ gtfs-realtime.proto
python -m grpc_tools.protoc --proto_path=./ --python_out=../ nyct-subway.proto

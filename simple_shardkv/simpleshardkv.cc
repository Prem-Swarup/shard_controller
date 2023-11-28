#include "simpleshardkv.h"

::grpc::Status SimpleShardkvServer::Get(::grpc::ServerContext* context,
                                        const ::GetRequest* request,
                                        ::GetResponse* response) {
  // request->key();
  mtx.lock();
  if(hashmap.find(request->key()) != hashmap.end())
  {
    // std::string data = hashmap[request->key()];
    response->set_data(hashmap[request->key()]);
    mtx.unlock();
    return ::grpc::Status::OK;
  }
  else
  {
    mtx.unlock();
    return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "<error: simple_get>");
  }
  // return ::grpc::Status(::grpc::StatusCode::UNIMPLEMENTED, "not implemented");
}

::grpc::Status SimpleShardkvServer::Put(::grpc::ServerContext* context,
                                        const ::PutRequest* request,
                                        Empty* response) {
  //don't exist
  mtx.lock();
  if(hashmap.find(request->key()) == hashmap.end())
  {
    hashmap.insert(std::pair<std::string,std::string>(request->key(), request->data()));
    mtx.unlock();
    return ::grpc::Status::OK;
  }
  else
  {
    hashmap[request->key()] = request->data();
    mtx.unlock();
    return ::grpc::Status::OK;
  }
  return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "<error: simple_put>");
  // return ::grpc::Status(::grpc::StatusCode::UNIMPLEMENTED, "not implemented");
}

::grpc::Status SimpleShardkvServer::Append(::grpc::ServerContext* context,
                                           const ::AppendRequest* request,
                                           Empty* response) {
  mtx.lock();
  if(hashmap.find(request->key()) != hashmap.end())
  {
    hashmap[request->key()] += request->data();
    mtx.unlock();
    return ::grpc::Status::OK;
  }
  else
  {
    hashmap.insert(std::pair<std::string,std::string>(request->key(), request->data()));
    mtx.unlock();
    return ::grpc::Status::OK;
  }
  return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "<error: simple_append>");
  // return ::grpc::Status(::grpc::StatusCode::UNIMPLEMENTED, "not implemented");
}

::grpc::Status SimpleShardkvServer::Delete(::grpc::ServerContext* context,
                                           const ::DeleteRequest* request,
                                           Empty* response) {
  mtx.lock();
  if(hashmap.find(request->key()) != hashmap.end())
  {
    hashmap.erase(request->key());
    mtx.unlock();
    return ::grpc::Status::OK;
  }
  else
  {
    mtx.unlock();
    return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "<error: simple_delete>");
  }
  // return ::grpc::Status(::grpc::StatusCode::UNIMPLEMENTED, "not implemented");
}

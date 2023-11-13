import uuid
import json
import redis
from fastapi import FastAPI
from fastapi import HTTPException
from pydantic import BaseModel
from redis import StrictRedis

# python3 -m uvicorn faas_server:app --reload
class RegisterFn(BaseModel):
    name: str
    payload: str

class RegisterFnRep(BaseModel):
    function_id: uuid.UUID

class ExecuteFnReq(BaseModel):
    function_id: uuid.UUID
    payload: str

class ExecuteFnRep(BaseModel):
    task_id: uuid.UUID

class TaskStatusRep(BaseModel):
    task_id: uuid.UUID
    status: str

class TaskResultRep(BaseModel):
    task_id: uuid.UUID
    status: str
    result: str


class TaskInfo(BaseModel):
    fn_payload : str
    param_payload: str 
    status: str
    result: str


app = FastAPI()

redis_conn = redis.StrictRedis(host='localhost', port=6379, db=0) 

@app.get("/result/{task_id}", response_model=TaskResultRep)
async def result(task_id):
    try:
        info = json.loads(redis_conn.get(task_id))
        return TaskResultRep(task_id, info["status"], info["result"])
    except Exception as e:
        raise HTTPException(status_code=500, detail="Failed to retrieve result from Redis")


@app.get("/status/{task_id}", response_model=TaskStatusRep)
async def status(task_id):
    try:
        info = json.loads(redis_conn.get(task_id))
        return TaskStatusRep(task_id, info["status"])
    except Exception as e:
        raise HTTPException(status_code=500, detail="Failed to retrieve status from Redis")


@app.post("/register_function", response_model=RegisterFnRep)
async def register(input : RegisterFn):
    try:
        uuid_key = uuid.uuid4()
        key_set_success = redis_conn.set(uuid_key, json.dumps(input))
        if key_set_success:
            return RegisterFnRep(uuid=uuid_key)
        else:
            raise HTTPException(status_code=500, detail="Failed to register function")
    except Exception as e:
        raise HTTPException(status_code=500, detail="Failed to register function")



@app.post("/execute_function", response_model=ExecuteFnRep)
async def execute(input : ExecuteFnReq):
    try:
        uuid_key = input["function_id"]
        function = json.loads(redis_conn.get(uuid_key))
        task_id = uuid.uuid4()
        info = TaskInfo(fn_payload=function['payload'], param_payload=input["payload"],
                 status="QUEUED", result=None)
        redis_conn.set(task_id, info)
        redis_conn.publish('Tasks', task_id)
        return ExecuteFnRep(task_id)
    except Exception as e:
        raise HTTPException(status_code=500, detail="Failed to execute function")



import uuid
import json
import redis
from fastapi import FastAPI
from fastapi import HTTPException
from pydantic import BaseModel
from redis import StrictRedis
from model import TaskInfo

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


app = FastAPI()
redis_conn = redis.StrictRedis(host='localhost', port=6379, password='garen', db=0)



@app.get("/result/{task_id}", response_model=TaskResultRep)
async def result(task_id):
    try:
        info = json.loads(redis_conn.get(str(task_id)))
        return {'task_id' :str(task_id), 'status' : info["status"], 'result' : info["result"]}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to retrieve result from Redis {e}")


@app.get("/status/{task_id}", response_model=TaskStatusRep)
async def status(task_id):
    try:
        info = json.loads(redis_conn.get(str(task_id)))
        return {'task_id' : task_id, 'status' : info["status"]}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to retrieve status from Redis {e}")


@app.post("/register_function", response_model=RegisterFnRep)
async def register(input : RegisterFn):
    try:
        uuid_key = uuid.uuid4()
        obj = json.dumps(input.dict())
        key_set_success = redis_conn.set(str(uuid_key), obj)
        if key_set_success:
            return {"function_id": uuid_key}
        else:
            raise HTTPException(status_code=500, detail="Failed to register function in redis")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to register function {e}")



@app.post("/execute_function", response_model=ExecuteFnRep)
async def execute(input : ExecuteFnReq):
    try:
        uuid_key = input.function_id
        function = json.loads(redis_conn.get(str(uuid_key)))
        task_id = uuid.uuid4()
        info = TaskInfo(fn_payload=function['payload'], param_payload=input.payload,
                 status="QUEUED", result='')
        redis_conn.set(str(task_id), json.dumps(info.dict()))
        redis_conn.publish('Tasks', str(task_id))
        return {"task_id": task_id}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to execute function {e}")



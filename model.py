from pydantic import BaseModel
import json

class TaskInfo(BaseModel):
  fn_payload : str
  param_payload: str 
  status: str
  result: str

  def model_dump_json(self):
    return json.dumps(self.dict())

class Task(BaseModel):
  task_id: str
  task_info: TaskInfo
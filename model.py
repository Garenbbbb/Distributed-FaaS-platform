from pydantic import BaseModel

class TaskInfo(BaseModel):
  fn_payload : str
  param_payload: str 
  status: str
  result: str

class Task(BaseModel):
  task_id: str
  task_info: TaskInfo
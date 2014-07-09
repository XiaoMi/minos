import deploy_utils
import threading

class DeployThread(threading.Thread):
  def __init__(self, func, para_list_set, name=''):
    threading.Thread.__init__(self)
    self.name = name
    self.func = func
    self.para_list_set = para_list_set

  def run(self):
    for task_id in range(len(self.para_list_set)):
      apply(self.func, self.para_list_set[task_id])


def start_deploy_threads(func, task_list):
  parallelism = len(task_list)
  threads = []
  for thread_id in range(parallelism):
    deploy_thread = DeployThread(func, para_list_set=task_list[thread_id])
    threads.append(deploy_thread)

  for index in range(parallelism):
    threads[index].start()

  for index in range(parallelism):
    threads[index].join()



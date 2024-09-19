from pyVmomi import  vim, vmodl
import time

def wait_for_vim_task(task):
    progress = None
    iter_count = 0
    while task.info.state not in (vim.TaskInfo.State.success, vim.TaskInfo.State.error):
        iter_count+=1
        print('task {}- status : {} - count: {}'.format(task.info,task.info.state,iter_count))
        if task.info.progress != progress:
            print('%s progress=%s' % (task, task.info.progress))
        if task.info.state != 'running':
            print('task state is not expected:\n %s' % task.info)
        progress = task.info.progress
        time.sleep(5)

def get_all_tasks_of_host(si, host):
    content = si.RetrieveContent()
    taskManager = content.taskManager
    filterspec = vim.TaskFilterSpec(entity=vim.TaskFilterSpec.ByEntity(entity=host, recursion='self'))
    collector = taskManager.CreateCollectorForTasks(filter=filterspec)
    collector.ResetCollector()
    alltasks = collector.ReadNextTasks(999)
    return alltasks

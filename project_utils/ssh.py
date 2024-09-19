import traceback
import paramiko
import select
import time
from project_utils.conversions import TextToString
from collections import defaultdict

class SSHSession(paramiko.SSHClient):
   """
   sub class of paramiko.SSHClient. Provides ability to read stream
   data from stdout, stderr in real time. Leverage ssh session
   multiplexing to send receive data over multiple channels.
   """

   def __init__(self, server, username, password, keep_alive=30, invoke_shell=False):
      super(SSHSession, self).__init__()
      self.server = server
      self.username = username
      self.password = password
      self.keepAlive = keep_alive
      self.channel = None
      self.establish_session(invoke_shell)

   def establish_session(self, invoke_shell):
      self.set_missing_host_key_policy(paramiko.AutoAddPolicy())
      attempts = 0
      while True:
         try:
            attempts += 1
            self.connect(self.server, username=self.username, password=self.password,
                         look_for_keys=False, allow_agent=False)
            break
         except paramiko.AuthenticationException:
            raise
         except Exception as ex:
            if attempts >= 3:
               raise
            print(
               'Connection attempt {} encounter {}. Sleep 3 seconds and retrying...'.format(
                  attempts, ex))
            time.sleep(3)
            continue
      # self._transport.set_keepalive(self.keepAlive)

      if invoke_shell:
         self.channel = self._transport.open_session(timeout=30)
         self.channel.get_pty()
         self.channel.set_combine_stderr(True)
         self.channel.invoke_shell()

   def is_active(self):
      return self._transport.is_active()

   def send(self, cmd, channel=None):
      """
      send command over paramiko.channel and keep channel open.
      :param cmd: command to execute
      :param channel: paramiko.channel object
      :return: None
      """
      channel = channel or self.channel
      if '\n' not in cmd:
         cmd += '\n'
      channel.send(cmd)

   def receive_stdout(self, channel=None, timeout=5, decode=False):
      """
      blocking while read stream data from channel.recv_ready and return
      aggregated data read from the channel.recv buffer.

      :param decode:  receive data to unicode
      :param channel: paramiko.channel object
      :param timeout: timeout waiting for channel file descriptor ready for read
      :return: unicode data
      """
      channel = channel or self.channel
      poll = select.poll()
      poll.register(channel, select.POLLIN)
      r = poll.poll(timeout)
      poll.unregister(channel)
      # r, w, x = select.select([channel], [], [], timeout)
      if not r:
         return
      data = ''
      try:
         while channel.recv_ready():
            buffersize = len(channel.in_buffer)
            if not buffersize:
               break
            buff = channel.recv(buffersize)
            if decode:
               data = buff.decode()
            else:
               data = buff
      except Exception:
         if buff:
            print('buffer len=%s' % len(buff))
         print(traceback.format_exc())
      return data

   def receive_stderr(self, channel=None, timeout=5, decode=False):
      """
      blocking while read stream data from channel.recv_stderr_ready and return
      aggregated data read from the channel.recv_stderr buffer.

      :param channel: paramiko.channel object
      :param timeout: timeout waiting for channel file descriptor ready for read
      :return: unicode data
      """
      channel = channel or self.channel
      poll = select.poll()
      poll.register(channel, select.POLLIN)
      r = poll.poll(timeout)
      poll.unregister(channel)
      # r, w, x = select.select([channel], [], [], timeout)
      if not r:
         return
      data = ''
      while channel.recv_stderr_ready():
         buffersize = len(channel.in_stderr_buffer)
         if not buffersize:
            break
         buff = channel.recv_stderr(buffersize)
         if decode:
            data = buff.decode()
         else:
            data = buff
      return data

   def exec_cmd(self, cmd, timeout=None, disable_log=False, umask=True, cmd_timeout=900):
      """
      execute command and return rc, stdout, stderr and close channel.
      Log stdout, stderr in real time

      :param cmd: the command to execute on remote host
      :return: int rc, str stdout, str stderr of the executed command
      """
      stdoutlines = ''
      stderrlines = ''
      if umask:
         cmd = 'umask 0; %s' % cmd
      if not disable_log:
         print('executing cmd %s on host %s' % (cmd, self.server))
      # stdin, stdout, stderr are stream objects
      stdin, stdout, stderr = self.exec_command(cmd, timeout=timeout)
      channel = stdout.channel
      channel.shutdown_write()
      # while not channel.eof_received or channel.recv_ready()\
      # or not stderr.channel.eof_received or stderr.channel.recv_ready():
      while True:
         start_time = time.time()
         time.sleep(0.5)
         if cmd_timeout <= 0:
            raise Exception('timeout waiting for cmd {}'.format(cmd))
         # if self._transport:
         #    transport = self.get_transport()
         #    transport.send_ignore()
         try:
            out = self.receive_stdout(channel=channel, decode=True)
            if out:
               stdoutlines += out
         except Exception:
            print(traceback.format_exc())
            channel.close()
            raise
         try:
            out = self.receive_stderr(channel=stderr.channel, decode=True)
            if out:
               stderrlines += out
         except:
            print(traceback.format_exc())
            stderr.channel.close()
            raise
         if channel.closed and stderr.channel.closed:
            # print('stdout.channel.closed={}; stderr.channel.closed={}'.format(channel.closed,
            #                                                                           stderr.channel.closed))
            # print('exit_status_ready={}'.format(channel.exit_status_ready()))
            # print('stdout_recv_ready={}'.format(channel.recv_ready()))
            # print('stderr_recv_ready={}'.format(stderr.channel.recv_ready()))
            if channel.exit_status_ready() and not channel.recv_ready() and not stderr.channel.recv_ready():
               break
         elapsed = int(time.time() - start_time)
         cmd_timeout -= elapsed
         if elapsed >= 60:
            print('{} timeout in {}'.format(cmd, cmd_timeout))
      channel.shutdown_read()
      stderr.channel.shutdown_read()
      rc = channel.recv_exit_status()
      if not disable_log:
         print('%s, %s, rc=%s, stdout=%s, stderr=%s' % (
            self.server, cmd, rc, stdoutlines, stderrlines))
      return rc, TextToString(stdoutlines), TextToString(stderrlines)


def RunSshCmdInSession(cmd, session, umask=False, raise_on_error=False, timeout=None,
                       cmd_timeout=900, disable_log=False):
   rc, stdout, stderr = session.exec_cmd(cmd, umask=umask, timeout=timeout, cmd_timeout=cmd_timeout,
                                         disable_log=disable_log)
   if raise_on_error and rc:
      raise Exception('failed to execute cmd=%s; rc=%s\n%s' % (cmd, rc, stdout + stderr))
   return rc, stdout, stderr


def RunSshCmd(cmd, host, user, pw, umask=False, raise_on_error=False, timeout=None,
              cmd_timeout=900, disable_log=False):

   with SSHSession(host, user, pw) as ssh:
      rc, stdout, stderr = ssh.exec_cmd(cmd, umask=umask, timeout=timeout, cmd_timeout=cmd_timeout,
                                        disable_log=disable_log)
      if raise_on_error and rc:
         raise Exception('failed to execute cmd=%s; rc=%s\n%s' % (cmd, rc, stdout + stderr))
      return rc, stdout, stderr


def run_ssh_cmd(host, cmd, user='root', password='ca$hc0w', timeout=120, log=False, raiseOnError=False ,esxcli=False, evaluation=True):
      msg = "Running ssh cmd {0} with {1} seconds timeout on {2}"
      if esxcli:
         cmd = "{bin} --formatter=python {cmd}".format(bin="/sbin/localcli", cmd=cmd)
      if log:
         print(msg.format(cmd, timeout, host))
      (rc, stdout, stderr) = RunSshCmd(cmd, host, user,
                                       password, cmd_timeout=timeout,
                                       disable_log=not log,
                                       raise_on_error=raiseOnError)
      if evaluation:
         if rc == 0 and len(stdout) > 0:
            return eval(stdout)
         else:
            return stderr
      return (rc, stdout, stderr)

def runVsiCommand(host, nodePath, command_type='get', value=None, evaluation=True):
   if command_type == 'get':
      vsi_get_template = 'python -c "import json; from vmware import vsi; x=vsi.{command_type}(\'{nodePath}\');print(json.dumps(x))"\n'
      vsi_cmd = vsi_get_template.format(command_type=command_type, nodePath=nodePath)
   else:
      if not value:
         raise Exception('Set have to be called with a value')
      vsi_set_template = 'python -c "import json; from vmware import vsi; x=vsi.{command_type}(\'{nodePath}\', {value});print(json.dumps(x))"\n'
      vsi_cmd = vsi_set_template.format(command_type=command_type, nodePath=nodePath, value=value)
   if evaluation:
      try:
         output = run_ssh_cmd(host, vsi_cmd)
         if isinstance(output, str) and 'traceback' in output.lower():
            raise Exception('Bad commands')
      except Exception as ex:
         print(f'cannot complete {vsi_cmd} on {host}: error - {ex}')
         output = dict()
      return output
   else:
      return run_ssh_cmd(host, vsi_cmd, evaluation=evaluation)

def run_cmd(host,cmd, username='root', password='ca$hc0w'):
   rc, stdout, stderr = run_ssh_cmd(host=host, cmd=cmd, evaluation=False, password=password, user=username)

   if rc != 0:
      err_msg = 'Error while executing - {cmd} - {host} - {error}'.format(
         cmd=cmd, host=host, error=stderr
      )
      print(err_msg)
      raise Exception(err_msg)
   if len(stdout.strip()):
      return stdout
   else:
      return None

if __name__ == '__main__':
   cmd = 'ls /mnt/fslvsdb/subha'
   out = run_ssh_cmd(host='172.24.113.88', cmd=cmd, evaluation=False)
   print(out)
